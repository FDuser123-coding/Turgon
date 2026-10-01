// Package plugin runs logic plugins (architecture §18.4): WebAssembly
// modules built for the WIT world turgon:stack/logic-plugin, version 0.2.0
// (wit/turgon-stack.wit) or 0.1.0 (wit/0.1.0). A plugin reacts to an
// event, may read entities, propose changes, publish events and (0.2.0)
// call the HTTPS hosts it is granted, and nothing else: it gets no WASI, so
// no files, clock, randomness or environment, and every call it makes is
// checked against the grants an administrator approved.
//
// Modules are what wit-bindgen produces for the world (a core module using
// the Canonical ABI, e.g. Rust built for wasm32-unknown-unknown), or a
// component built from one (`wasm-tools component new`), whose core module
// the host runs. The host speaks the Canonical ABI for the world's few
// types itself.
package plugin

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

// World is the newest WIT world this host implements; Worlds lists them all.
const World = "turgon:stack/logic-plugin@0.2.0"

var Worlds = []string{"turgon:stack/logic-plugin@0.1.0", World}

const (
	worldPrefix   = "turgon:stack/logic-plugin@"
	maxModuleSize = 8 << 20
	// Per invocation.
	maxHostCalls = 64
	maxString    = 1 << 20 // a string or list a plugin passes the host
	maxErrorText = 4 << 10
)

var (
	i32 = api.ValueTypeI32
	// The world's imports, lowered by the Canonical ABI: strings are
	// (pointer, length) pairs, and a result too large for one value is
	// written to a pointer the caller passes last.
	signatures = map[string][]api.ValueType{
		"entities.get":            {i32, i32, i32, i32, i32},
		"entities.propose-change": {i32, i32, i32, i32, i32, i32, i32},
		"events.publish":          {i32, i32, i32, i32, i32},
		// method, url, headers, body: (pointer, length) each; the result.
		"http.send": {i32, i32, i32, i32, i32, i32, i32, i32, i32},
	}
	// What each version of the world imports.
	versions = map[string][]string{
		"0.1.0": {"entities.get", "entities.propose-change", "events.publish"},
		"0.2.0": {"entities.get", "entities.propose-change", "events.publish", "http.send"},
	}
)

// importOf splits a host import such as turgon:stack/entities@0.2.0.get
// into "entities.get" and "0.2.0".
func importOf(module, name string) (fn, version string, ok bool) {
	iface, ok := strings.CutPrefix(module, "turgon:stack/")
	if !ok {
		return "", "", false
	}
	iface, version, ok = strings.Cut(iface, "@")
	fn = iface + "." + name
	return fn, version, ok && slices.Contains(versions[version], fn)
}

// ErrorCode is the world's error-code variant.
type ErrorCode struct {
	Kind    ErrorKind
	Message string // for Invalid and Unavailable
}

type ErrorKind uint8

const (
	Denied ErrorKind = iota
	NotFound
	Invalid
	// Unavailable (0.2.0): a service did not answer, or answered with an
	// error. Plugins built for 0.1.0 see it as Invalid.
	Unavailable
)

func (e *ErrorCode) Error() string {
	switch e.Kind {
	case Denied:
		return "denied"
	case NotFound:
		return "not found"
	case Unavailable:
		return "unavailable: " + e.Message
	}
	return "invalid: " + e.Message
}

// Host answers a plugin's calls. Errors other than *ErrorCode abort the
// invocation: they mean the host failed, not that the plugin asked for
// something it may not have.
type Host interface {
	Get(ctx context.Context, kind, id string) (string, error)
	ProposeChange(ctx context.Context, kind, id, patch string) (string, error)
	Publish(ctx context.Context, topic string, payload []byte) error
	Send(ctx context.Context, req HTTPRequest) (*HTTPResponse, error)
}

// Header is an HTTP header, in the order the plugin or the server gave it.
type Header struct{ Name, Value string }

// HTTPRequest and HTTPResponse are the world's http records.
type HTTPRequest struct {
	Method, URL string
	Headers     []Header
	Body        []byte
}

type HTTPResponse struct {
	Status  uint16
	Headers []Header
	Body    []byte
}

// Limits bound one invocation.
type Limits struct {
	MemoryMB int
	Timeout  time.Duration
}

// Module is a compiled plugin, ready to handle events.
type Module struct {
	rt       wazero.Runtime
	compiled wazero.CompiledModule
	limits   Limits
	// Imports lists the world functions the module uses, e.g.
	// "entities.get"; World is the world they come from, empty if it
	// imports nothing.
	Imports []string
	World   string
	hasPost bool
}

// ErrPlugin wraps what the plugin itself got wrong or reported: a trap, a
// limit it hit, or the error its handle function returned. Such failures
// are not retried.
var ErrPlugin = errors.New("plugin")

// Load validates and compiles a module. It accepts a core module or a
// component wrapping one, and refuses any import outside the world.
func Load(ctx context.Context, wasm []byte, limits Limits) (*Module, error) {
	if len(wasm) > maxModuleSize {
		return nil, fmt.Errorf("the module is %d bytes; at most %d", len(wasm), maxModuleSize)
	}
	cores, err := coreModules(wasm)
	if err != nil {
		return nil, err
	}
	if limits.MemoryMB <= 0 || limits.Timeout <= 0 {
		return nil, errors.New("memory and time limits are required")
	}
	cfg := wazero.NewRuntimeConfig().
		WithMemoryLimitPages(uint32(limits.MemoryMB) * 16). // 64 KiB pages
		WithCloseOnContextDone(true).
		WithCoreFeatures(api.CoreFeaturesV2)
	rt := wazero.NewRuntimeWithConfig(ctx, cfg)
	m := &Module{rt: rt, limits: limits}
	ok := false
	defer func() {
		if !ok {
			_ = rt.Close(ctx)
		}
	}()
	var compiled wazero.CompiledModule
	for _, core := range cores {
		c, err := rt.CompileModule(ctx, core)
		if err != nil {
			return nil, fmt.Errorf("not a valid WebAssembly module: %w", err)
		}
		if _, ok := c.ExportedFunctions()["handle"]; !ok && len(cores) > 1 {
			_ = c.Close(ctx) // component glue
			continue
		}
		if compiled != nil {
			return nil, errors.New("the component holds more than one module exporting handle")
		}
		compiled = c
	}
	if compiled == nil {
		return nil, errors.New("no module in the component exports handle as turgon:stack/logic-plugin defines it")
	}
	version := ""
	for _, f := range compiled.ImportedFunctions() {
		mod, name, _ := f.Import()
		fn, v, known := importOf(mod, name)
		if !known {
			if mod == "wasi_snapshot_preview1" || mod == "wasi_unstable" {
				return nil, fmt.Errorf("the module imports WASI (%s.%s): plugins get no files, clock, randomness or environment; build for wasm32-unknown-unknown", mod, name)
			}
			return nil, fmt.Errorf("the module imports %s.%s, which no version of turgon:stack/logic-plugin (%s) offers", mod, name, strings.Join(Worlds, ", "))
		}
		if version != "" && v != version {
			return nil, fmt.Errorf("the module mixes versions %s and %s of turgon:stack", version, v)
		}
		version = v
		if !sameTypes(f.ParamTypes(), signatures[fn]) || len(f.ResultTypes()) != 0 {
			return nil, fmt.Errorf("%s.%s has the wrong signature for %s%s", mod, name, worldPrefix, v)
		}
		m.Imports = append(m.Imports, fn)
	}
	if version != "" {
		m.World = worldPrefix + version
	}
	sort.Strings(m.Imports)
	for _, mem := range compiled.ImportedMemories() {
		mod, name, _ := mem.Import()
		return nil, fmt.Errorf("the module imports memory %s.%s; it must define its own", mod, name)
	}
	exports := compiled.ExportedFunctions()
	if f, ok := exports["handle"]; !ok || !sameTypes(f.ParamTypes(), []api.ValueType{i32, i32}) || !sameTypes(f.ResultTypes(), []api.ValueType{i32}) {
		return nil, errors.New("the module does not export handle as turgon:stack/logic-plugin defines it")
	}
	if f, ok := exports["cabi_realloc"]; !ok || !sameTypes(f.ParamTypes(), []api.ValueType{i32, i32, i32, i32}) || !sameTypes(f.ResultTypes(), []api.ValueType{i32}) {
		return nil, errors.New("the module does not export cabi_realloc")
	}
	if f, ok := exports["cabi_post_handle"]; ok {
		if !sameTypes(f.ParamTypes(), []api.ValueType{i32}) || len(f.ResultTypes()) != 0 {
			return nil, errors.New("cabi_post_handle has the wrong signature")
		}
		m.hasPost = true
	}
	if _, ok := compiled.ExportedMemories()["memory"]; !ok {
		return nil, errors.New("the module does not export its memory")
	}
	if err := m.hostModules(ctx); err != nil {
		return nil, err
	}
	m.compiled = compiled
	ok = true
	return m, nil
}

func sameTypes(a, b []api.ValueType) bool { return bytes.Equal(a, b) }

// Close releases the module.
func (m *Module) Close(ctx context.Context) error { return m.rt.Close(ctx) }

// call is one invocation's state, reached by the host functions through
// the context.
type call struct {
	host    Host
	calls   int
	err     error // a host failure that aborted the invocation
	stopped error // what the plugin did wrong in a host call
}

type callKey struct{}

// Handle runs the plugin on one event, in a fresh instance: nothing
// survives from one event to the next.
func (m *Module) Handle(ctx context.Context, event []byte, host Host) error {
	ctx, cancel := context.WithTimeout(ctx, m.limits.Timeout)
	defer cancel()
	c := &call{host: host}
	ctx = context.WithValue(ctx, callKey{}, c)
	inst, err := m.rt.InstantiateModule(ctx, m.compiled, wazero.NewModuleConfig().WithName("").WithStartFunctions())
	if err != nil {
		return m.failure(ctx, c, fmt.Errorf("could not start: %w", err))
	}
	defer inst.Close(context.Background())
	ptr, err := put(ctx, inst, event)
	if err != nil {
		return m.failure(ctx, c, err)
	}
	res, err := inst.ExportedFunction("handle").Call(ctx, uint64(ptr), uint64(len(event)))
	if err != nil {
		return m.failure(ctx, c, err)
	}
	ret := uint32(res[0])
	disc, ok := inst.Memory().ReadByte(ret)
	if !ok {
		return fmt.Errorf("%w: handle returned an invalid pointer", ErrPlugin)
	}
	var out error
	switch disc {
	case 0:
	case 1:
		msg, err := readString(inst, ret+4, maxErrorText)
		if err != nil {
			return fmt.Errorf("%w: handle failed with an unreadable error: %v", ErrPlugin, err)
		}
		out = fmt.Errorf("%w: %s", ErrPlugin, msg)
	default:
		return fmt.Errorf("%w: handle returned an invalid result", ErrPlugin)
	}
	if m.hasPost {
		if _, err := inst.ExportedFunction("cabi_post_handle").Call(ctx, uint64(ret)); err != nil {
			return m.failure(ctx, c, err)
		}
	}
	return out
}

// failure explains why an invocation stopped.
func (m *Module) failure(ctx context.Context, c *call, err error) error {
	switch {
	case c.err != nil:
		return c.err // the host failed: retryable
	case c.stopped != nil:
		return c.stopped
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return fmt.Errorf("%w: ran longer than %s", ErrPlugin, m.limits.Timeout)
	}
	return fmt.Errorf("%w: %v", ErrPlugin, err)
}

// put copies bytes into the guest's memory, allocated by its cabi_realloc.
func put(ctx context.Context, inst api.Module, b []byte) (uint32, error) {
	if len(b) == 0 {
		return 1, nil // any aligned pointer will do for an empty list
	}
	res, err := inst.ExportedFunction("cabi_realloc").Call(ctx, 0, 0, 1, uint64(len(b)))
	if err != nil {
		return 0, err
	}
	ptr := uint32(res[0])
	if !inst.Memory().Write(ptr, b) {
		return 0, fmt.Errorf("%w: cabi_realloc returned an invalid pointer", ErrPlugin)
	}
	return ptr, nil
}

// readString reads a (pointer, length) pair at addr, then the bytes.
func readString(inst api.Module, addr uint32, limit int) (string, error) {
	b, err := readList(inst, addr, limit)
	if err != nil {
		return "", err
	}
	if !utf8.Valid(b) {
		return "", errors.New("not UTF-8")
	}
	return string(b), nil
}

func readList(inst api.Module, addr uint32, limit int) ([]byte, error) {
	ptr, ok1 := inst.Memory().ReadUint32Le(addr)
	n, ok2 := inst.Memory().ReadUint32Le(addr + 4)
	if !ok1 || !ok2 {
		return nil, errors.New("out of bounds")
	}
	return readBytes(inst, ptr, n, limit)
}

func readBytes(inst api.Module, ptr, n uint32, limit int) ([]byte, error) {
	if int(n) > limit {
		return nil, fmt.Errorf("%d bytes; at most %d", n, limit)
	}
	b, ok := inst.Memory().Read(ptr, n)
	if !ok {
		return nil, errors.New("out of bounds")
	}
	return bytes.Clone(b), nil
}

// stop ends the invocation from a host function: the plugin passed
// something invalid or hit a limit.
func stop(ctx context.Context, err error) {
	if c, _ := ctx.Value(callKey{}).(*call); c != nil && c.stopped == nil && c.err == nil {
		c.stopped = err
	}
	panic(err)
}

func (m *Module) hostModules(ctx context.Context) error {
	for version, fns := range versions {
		v1 := version == "0.1.0"
		builders := map[string]wazero.HostModuleBuilder{}
		for _, fn := range fns {
			iface, name, _ := strings.Cut(fn, ".")
			b, ok := builders[iface]
			if !ok {
				b = m.rt.NewHostModuleBuilder("turgon:stack/" + iface + "@" + version)
				builders[iface] = b
			}
			b.NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(hostFunc(fn, v1)), signatures[fn], nil).Export(name)
		}
		for _, b := range builders {
			if _, err := b.Instantiate(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

// hostFunc implements one world function over the Canonical ABI.
func hostFunc(fn string, v1 bool) func(ctx context.Context, mod api.Module, s []uint64) {
	u := func(v uint64) uint32 { return uint32(v) }
	switch fn {
	case "entities.get":
		return func(ctx context.Context, mod api.Module, s []uint64) {
			c := begin(ctx)
			kind, id := str(ctx, mod, u(s[0]), u(s[1])), str(ctx, mod, u(s[2]), u(s[3]))
			v, err := c.host.Get(ctx, kind, id)
			writeResult(ctx, mod, c, u(s[4]), v1, smallResult, stringAt4(ctx, mod, v), err)
		}
	case "entities.propose-change":
		return func(ctx context.Context, mod api.Module, s []uint64) {
			c := begin(ctx)
			kind, id, patch := str(ctx, mod, u(s[0]), u(s[1])), str(ctx, mod, u(s[2]), u(s[3])), str(ctx, mod, u(s[4]), u(s[5]))
			v, err := c.host.ProposeChange(ctx, kind, id, patch)
			writeResult(ctx, mod, c, u(s[6]), v1, smallResult, stringAt4(ctx, mod, v), err)
		}
	case "events.publish":
		return func(ctx context.Context, mod api.Module, s []uint64) {
			c := begin(ctx)
			topic := str(ctx, mod, u(s[0]), u(s[1]))
			payload, err := readBytes(mod, u(s[2]), u(s[3]), maxString)
			if err != nil {
				stop(ctx, fmt.Errorf("%w: the payload: %v", ErrPlugin, err))
			}
			writeResult(ctx, mod, c, u(s[4]), v1, smallResult, nil, c.host.Publish(ctx, topic, payload))
		}
	case "http.send":
		return func(ctx context.Context, mod api.Module, s []uint64) {
			c := begin(ctx)
			req := HTTPRequest{Method: str(ctx, mod, u(s[0]), u(s[1])), URL: str(ctx, mod, u(s[2]), u(s[3])),
				Headers: readHeaders(ctx, mod, u(s[4]), u(s[5]))}
			body, err := readBytes(mod, u(s[6]), u(s[7]), maxString)
			if err != nil {
				stop(ctx, fmt.Errorf("%w: the request body: %v", ErrPlugin, err))
			}
			req.Body = body
			resp, err := c.host.Send(ctx, req)
			var ok func([]byte)
			if err == nil {
				ok = func(buf []byte) {
					binary.LittleEndian.PutUint16(buf[4:], resp.Status)
					hp, hn := writeHeaders(ctx, mod, resp.Headers)
					binary.LittleEndian.PutUint32(buf[8:], hp)
					binary.LittleEndian.PutUint32(buf[12:], hn)
					bp := alloc(ctx, mod, resp.Body)
					binary.LittleEndian.PutUint32(buf[16:], bp)
					binary.LittleEndian.PutUint32(buf[20:], uint32(len(resp.Body)))
				}
			}
			writeResult(ctx, mod, c, u(s[8]), v1, responseResult, ok, err)
		}
	}
	panic("unknown world function " + fn)
}

const maxHeaders = 64

// stringAt4 writes a string result's payload.
func stringAt4(ctx context.Context, mod api.Module, v string) func([]byte) {
	return func(buf []byte) {
		binary.LittleEndian.PutUint32(buf[4:], alloc(ctx, mod, []byte(v)))
		binary.LittleEndian.PutUint32(buf[8:], uint32(len(v)))
	}
}

func str(ctx context.Context, mod api.Module, ptr, n uint32) string {
	b, err := readBytes(mod, ptr, n, maxString)
	if err != nil || !utf8.Valid(b) {
		stop(ctx, fmt.Errorf("%w: an argument is not a valid string", ErrPlugin))
	}
	return string(b)
}

func begin(ctx context.Context) *call {
	c, _ := ctx.Value(callKey{}).(*call)
	if c == nil {
		panic(errors.New("host call outside an invocation"))
	}
	c.calls++
	if c.calls > maxHostCalls {
		stop(ctx, fmt.Errorf("%w: more than %d host calls", ErrPlugin, maxHostCalls))
	}
	return c
}

// alloc copies bytes into guest memory for a result.
func alloc(ctx context.Context, mod api.Module, b []byte) uint32 {
	p, err := put(ctx, mod, b)
	if err != nil {
		stop(ctx, fmt.Errorf("%w: could not take a result: %v", ErrPlugin, err))
	}
	return p
}

// readHeaders reads a list<header>: records of two strings, 16 bytes each.
func readHeaders(ctx context.Context, mod api.Module, ptr, n uint32) []Header {
	if n > maxHeaders {
		stop(ctx, fmt.Errorf("%w: more than %d headers", ErrPlugin, maxHeaders))
	}
	out := make([]Header, 0, n)
	for i := uint32(0); i < n; i++ {
		at := ptr + 16*i
		mem := mod.Memory()
		np, ok1 := mem.ReadUint32Le(at)
		nn, ok2 := mem.ReadUint32Le(at + 4)
		vp, ok3 := mem.ReadUint32Le(at + 8)
		vn, ok4 := mem.ReadUint32Le(at + 12)
		if !ok1 || !ok2 || !ok3 || !ok4 {
			stop(ctx, fmt.Errorf("%w: a header is out of bounds", ErrPlugin))
		}
		out = append(out, Header{Name: str(ctx, mod, np, nn), Value: str(ctx, mod, vp, vn)})
	}
	return out
}

// writeHeaders writes a list<header> into guest memory.
func writeHeaders(ctx context.Context, mod api.Module, hs []Header) (uint32, uint32) {
	if len(hs) > maxHeaders {
		hs = hs[:maxHeaders]
	}
	if len(hs) == 0 {
		return 4, 0
	}
	res, err := mod.ExportedFunction("cabi_realloc").Call(ctx, 0, 0, 4, uint64(16*len(hs)))
	if err != nil {
		stop(ctx, fmt.Errorf("%w: could not take a result: %v", ErrPlugin, err))
	}
	list := uint32(res[0])
	var rec [16]byte
	for i, h := range hs {
		binary.LittleEndian.PutUint32(rec[0:], alloc(ctx, mod, []byte(h.Name)))
		binary.LittleEndian.PutUint32(rec[4:], uint32(len(h.Name)))
		binary.LittleEndian.PutUint32(rec[8:], alloc(ctx, mod, []byte(h.Value)))
		binary.LittleEndian.PutUint32(rec[12:], uint32(len(h.Value)))
		if !mod.Memory().Write(list+16*uint32(i), rec[:]) {
			stop(ctx, fmt.Errorf("%w: cabi_realloc returned an invalid pointer", ErrPlugin))
		}
	}
	return list, uint32(len(hs))
}

// Result sizes: result<string, error-code> and result<_, error-code> take
// 16 bytes (an error-code is a discriminant and a string); result<response,
// error-code> 24.
const (
	smallResult    = 16
	responseResult = 24
)

// writeResult writes a result<T, error-code> of size bytes at ret: the
// discriminant at 0, the payload at 4 (ok writes T's).
func writeResult(ctx context.Context, mod api.Module, c *call, ret uint32, v1 bool, size int, ok func([]byte), err error) {
	buf := make([]byte, size)
	var ec *ErrorCode
	switch {
	case err == nil:
		if ok != nil {
			ok(buf)
		}
	case errors.As(err, &ec):
		kind, msg := ec.Kind, ec.Message
		if kind == Unavailable && v1 {
			kind, msg = Invalid, "unavailable: "+msg
		}
		buf[0], buf[4] = 1, byte(kind)
		if kind == Invalid || kind == Unavailable {
			binary.LittleEndian.PutUint32(buf[8:], alloc(ctx, mod, []byte(msg)))
			binary.LittleEndian.PutUint32(buf[12:], uint32(len(msg)))
		}
	default:
		c.err = err
		panic(err)
	}
	if !mod.Memory().Write(ret, buf) {
		stop(ctx, fmt.Errorf("%w: invalid return pointer", ErrPlugin))
	}
}
