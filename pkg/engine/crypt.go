package engine

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
)

// Temporal keeps every run's input, activity results, signals and failure
// messages in its history: customer records, amounts, approvals. When
// Temporal runs outside the customer's network (Temporal Cloud, or a shared
// cluster), payload encryption keeps them readable only by Turgon: workers,
// the console and the agent servers encrypt what they send to Temporal and
// decrypt what they read back, with keys Temporal never sees.

const (
	encodingEncrypted = "binary/encrypted"
	metaEncoding      = "encoding"
	metaKeyID         = "turgon-key-id"
)

// Codec encrypts payloads with AES-256-GCM. The first key encrypts; every
// key decrypts, so keys can be rotated: add the new one first, and drop
// the old one once no run in retention still uses it.
type Codec struct {
	current string
	keys    map[string]cipher.AEAD
}

var keyIDRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// NewCodec parses keys as "id:base64key,id:base64key", each key 32 bytes.
func NewCodec(spec string) (*Codec, error) {
	c := &Codec{keys: map[string]cipher.AEAD{}}
	for _, part := range strings.Split(spec, ",") {
		id, b64, ok := strings.Cut(strings.TrimSpace(part), ":")
		if !ok || !keyIDRE.MatchString(id) {
			return nil, fmt.Errorf("payload keys: want id:base64key, got %q", strings.SplitN(part, ":", 2)[0])
		}
		key, err := base64.StdEncoding.DecodeString(b64)
		if err != nil || len(key) != 32 {
			return nil, fmt.Errorf("payload key %s: must be 32 bytes, base64-encoded (e.g. openssl rand -base64 32)", id)
		}
		if _, dup := c.keys[id]; dup {
			return nil, fmt.Errorf("payload key %s is listed twice", id)
		}
		block, _ := aes.NewCipher(key)
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, err
		}
		c.keys[id] = aead
		if c.current == "" {
			c.current = id
		}
	}
	return c, nil
}

// Encode implements converter.PayloadCodec.
func (c *Codec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	out := make([]*commonpb.Payload, len(payloads))
	aead := c.keys[c.current]
	for i, p := range payloads {
		plain, err := p.Marshal()
		if err != nil {
			return nil, err
		}
		nonce := make([]byte, aead.NonceSize())
		if _, err := rand.Read(nonce); err != nil {
			return nil, err
		}
		// The key ID is authenticated too, so it cannot be swapped.
		sealed := aead.Seal(nonce, nonce, plain, []byte(c.current))
		out[i] = &commonpb.Payload{
			Metadata: map[string][]byte{metaEncoding: []byte(encodingEncrypted), metaKeyID: []byte(c.current)},
			Data:     sealed,
		}
	}
	return out, nil
}

// Decode implements converter.PayloadCodec. Payloads written before
// encryption was turned on pass through, so existing runs keep working.
func (c *Codec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	out := make([]*commonpb.Payload, len(payloads))
	for i, p := range payloads {
		if string(p.GetMetadata()[metaEncoding]) != encodingEncrypted {
			out[i] = p
			continue
		}
		id := string(p.GetMetadata()[metaKeyID])
		aead, ok := c.keys[id]
		if !ok {
			return nil, fmt.Errorf("payload encrypted with key %q, which this process does not have", id)
		}
		data := p.GetData()
		if len(data) < aead.NonceSize() {
			return nil, errors.New("encrypted payload is truncated")
		}
		plain, err := aead.Open(nil, data[:aead.NonceSize()], data[aead.NonceSize():], []byte(id))
		if err != nil {
			return nil, fmt.Errorf("payload does not decrypt with key %q: %w", id, err)
		}
		var dec commonpb.Payload
		if err := dec.Unmarshal(plain); err != nil {
			return nil, err
		}
		out[i] = &dec
	}
	return out, nil
}

var (
	dataConverter    = converter.GetDefaultDataConverter()
	failureConverter = temporal.GetDefaultFailureConverter()
)

// EncryptPayloads makes every Temporal client this process creates, and
// every history it reads, use the codec. Failure messages are encrypted
// too: they can quote records ("customer x@y has no master record").
// Call it before Dial.
func EncryptPayloads(c *Codec) {
	dataConverter = converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), c)
	failureConverter = temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{
		DataConverter: dataConverter, EncodeCommonAttributes: true,
	})
}

// DataConverter encodes and decodes this process's Temporal payloads.
func DataConverter() converter.DataConverter { return dataConverter }

// FailureConverter turns failures in histories back into errors.
func FailureConverter() converter.FailureConverter { return failureConverter }

var _ converter.PayloadCodec = (*Codec)(nil)
