// Package plugin runs logic plugins (architecture §18.4): WebAssembly
// modules built for the WIT world turgon:stack/logic-plugin@0.1.0
// (wit/turgon-stack.wit). A plugin reacts to an event, may read entities
// and propose changes through the host, and nothing else: it gets no WASI,
// so no files, clock, randomness, environment or network, and every call
// it makes is checked against the grants an administrator approved.
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
	"sort"
	"time"
	"unicode/utf8"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

// World is the only WIT world this host implements.
const World = "turgon:stack/logic-plugin@0.1.0"

const (
	entitiesModule = "turgon:stack/entities@0.1.0"
	eventsModule   = "turgon:stack/events@0.1.0"
	maxModuleSize  = 8 << 20
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
	allowedImports = map[string]map[string][]api.ValueType{
		entitiesModule: {
			"get":            {i32, i32, i32, i32, i32},
			"propose-change": {i32, i32, i32, i32, i32, i32, i32},
		},
		eventsModule: {
			"publish": {i32, i32, i32, i32, i32},
		},
	}
)

// ErrorCode is the world's error-code variant.
type ErrorCode struct {
	Kind    ErrorKind
	Message string // for Invalid
}

type ErrorKind uint8

const (
	Denied ErrorKind = iota
	NotFound
	Invalid
)

func (e *ErrorCode) Error() string {
	switch e.Kind {
	case Denied:
		return "denied"
	case NotFound:
		return "not found"
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
	// "entities.get".
	Imports []string
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
		return nil, fmt.Errorf("no module in the component exports handle as %s defines it", World)
	}
	for _, f := range compiled.ImportedFunctions() {
		mod, name, _ := f.Import()
		want, known := allowedImports[mod][name]
		if !known {
			if mod == "wasi_snapshot_preview1" || mod == "wasi_unstable" {
				return nil, fmt.Errorf("the module imports WASI (%s.%s): plugins get no files, clock, randomness or network; build for wasm32-unknown-unknown", mod, name)
			}
			return nil, fmt.Errorf("the module imports %s.%s, which %s does not offer", mod, name, World)
		}
		if !sameTypes(f.ParamTypes(), want) || len(f.ResultTypes()) != 0 {
			return nil, fmt.Errorf("%s.%s has the wrong signature for %s", mod, name, World)
		}
		short := map[string]string{entitiesModule: "entities", eventsModule: "events"}[mod]
		m.Imports = append(m.Imports, short+"."+name)
	}
	sort.Strings(m.Imports)
	for _, mem := range compiled.ImportedMemories() {
		mod, name, _ := mem.Import()
		return nil, fmt.Errorf("the module imports memory %s.%s; it must define its own", mod, name)
	}
	exports := compiled.ExportedFunctions()
	if f, ok := exports["handle"]; !ok || !sameTypes(f.ParamTypes(), []api.ValueType{i32, i32}) || !sameTypes(f.ResultTypes(), []api.ValueType{i32}) {
		return nil, fmt.Errorf("the module does not export handle as %s defines it", World)
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
	str := func(ctx context.Context, mod api.Module, ptr, n uint32) string {
		b, err := readBytes(mod, ptr, n, maxString)
		if err != nil || !utf8.Valid(b) {
			stop(ctx, fmt.Errorf("%w: an argument is not a valid string", ErrPlugin))
		}
		return string(b)
	}
	begin := func(ctx context.Context) *call {
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
	// writeResult writes result<string, error-code> or result<_, error-code>
	// at ret: discriminant at 0, payload at 4.
	writeResult := func(ctx context.Context, mod api.Module, c *call, ret uint32, ok *string, err error) {
		mem := mod.Memory()
		var buf [16]byte
		var ec *ErrorCode
		switch {
		case err == nil:
			if ok != nil {
				p, perr := put(ctx, mod, []byte(*ok))
				if perr != nil {
					stop(ctx, fmt.Errorf("%w: could not take a result: %v", ErrPlugin, perr))
				}
				binary.LittleEndian.PutUint32(buf[4:], p)
				binary.LittleEndian.PutUint32(buf[8:], uint32(len(*ok)))
			}
		case errors.As(err, &ec):
			buf[0] = 1
			buf[4] = byte(ec.Kind)
			if ec.Kind == Invalid {
				p, perr := put(ctx, mod, []byte(ec.Message))
				if perr != nil {
					stop(ctx, fmt.Errorf("%w: could not take a result: %v", ErrPlugin, perr))
				}
				binary.LittleEndian.PutUint32(buf[8:], p)
				binary.LittleEndian.PutUint32(buf[12:], uint32(len(ec.Message)))
			}
		default:
			c.err = err
			panic(err)
		}
		if !mem.Write(ret, buf[:]) {
			stop(ctx, fmt.Errorf("%w: invalid return pointer", ErrPlugin))
		}
	}
	u := func(v uint64) uint32 { return uint32(v) }
	_, err := m.rt.NewHostModuleBuilder(entitiesModule).
		NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(func(ctx context.Context, mod api.Module, s []uint64) {
		c := begin(ctx)
		kind, id := str(ctx, mod, u(s[0]), u(s[1])), str(ctx, mod, u(s[2]), u(s[3]))
		v, err := c.host.Get(ctx, kind, id)
		writeResult(ctx, mod, c, u(s[4]), &v, err)
	}), allowedImports[entitiesModule]["get"], nil).Export("get").
		NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(func(ctx context.Context, mod api.Module, s []uint64) {
		c := begin(ctx)
		kind, id, patch := str(ctx, mod, u(s[0]), u(s[1])), str(ctx, mod, u(s[2]), u(s[3])), str(ctx, mod, u(s[4]), u(s[5]))
		v, err := c.host.ProposeChange(ctx, kind, id, patch)
		writeResult(ctx, mod, c, u(s[6]), &v, err)
	}), allowedImports[entitiesModule]["propose-change"], nil).Export("propose-change").
		Instantiate(ctx)
	if err != nil {
		return err
	}
	_, err = m.rt.NewHostModuleBuilder(eventsModule).
		NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(func(ctx context.Context, mod api.Module, s []uint64) {
		c := begin(ctx)
		topic := str(ctx, mod, u(s[0]), u(s[1]))
		payload, err := readBytes(mod, u(s[2]), u(s[3]), maxString)
		if err != nil {
			stop(ctx, fmt.Errorf("%w: the payload: %v", ErrPlugin, err))
		}
		writeResult(ctx, mod, c, u(s[4]), nil, c.host.Publish(ctx, topic, payload))
	}), allowedImports[eventsModule]["publish"], nil).Export("publish").
		Instantiate(ctx)
	return err
}
