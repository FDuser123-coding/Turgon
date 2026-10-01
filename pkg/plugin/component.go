package plugin

import (
	"bytes"
	"encoding/binary"
	"errors"
)

var (
	coreHeader      = []byte{0x00, 'a', 's', 'm', 0x01, 0x00, 0x00, 0x00}
	componentMagic  = []byte{0x00, 'a', 's', 'm'}
	componentLayer1 = []byte{0x01, 0x00} // layer 1: a component
)

// coreModules returns the core modules to choose the plugin from: the
// module itself, or the core modules a component wraps. Load runs the one
// that exports handle; the others are the glue wit-component generates
// (shim and fixup modules), which the host's own imports make unneeded.
func coreModules(b []byte) ([][]byte, error) {
	if bytes.HasPrefix(b, coreHeader) {
		return [][]byte{b}, nil
	}
	if len(b) < 8 || !bytes.Equal(b[:4], componentMagic) || !bytes.Equal(b[6:8], componentLayer1) {
		return nil, errors.New("not a WebAssembly module or component")
	}
	var modules [][]byte
	for rest := b[8:]; len(rest) > 0; {
		id := rest[0]
		size, n := binary.Uvarint(rest[1:])
		if n <= 0 || uint64(len(rest)-1-n) < size {
			return nil, errors.New("a malformed component")
		}
		body := rest[1+n : 1+n+int(size)]
		rest = rest[1+n+int(size):]
		switch id {
		case 1: // core:module
			modules = append(modules, body)
		case 4: // component: a nested one
			return nil, errors.New("the component nests components; build the plugin as one core module (wasm32-unknown-unknown)")
		}
	}
	for _, m := range modules {
		if !bytes.HasPrefix(m, coreHeader) {
			return nil, errors.New("a malformed component")
		}
	}
	if len(modules) == 0 {
		return nil, errors.New("the component holds no core module")
	}
	return modules, nil
}
