package rest

import (
	"net/http"

	"github.com/fduser123-coding/turgon/pkg/meta"
)

// LearnRefusals marks, in the objects the connection's writes send, the
// fields refused writes were missing. Without an OpenAPI description the
// records are sampled, and a sample cannot show that a field became
// required; the API's refusal of a write that lacks it can, and every
// other operation writing the same object is then reported by the drift.
func (c *Conn) LearnRefusals(cat *meta.Catalog, refusals []meta.Refusal) {
	for _, r := range refusals {
		op, ok := c.cfg.Operations[r.Operation]
		if !ok || op.Method == http.MethodGet || op.Restore {
			continue
		}
		fields := meta.RequiredFields(r.Error)
		if len(fields) == 0 {
			continue
		}
		for _, u := range cat.Uses {
			if u.By != "operation "+r.Operation && u.By != "operation "+r.Operation+" (writes)" {
				continue
			}
			for _, f := range fields {
				if f != topField(op.Wrap) { // Shopify's "order" missing is the wrapper, not a field
					meta.Learn(cat, u.Object, f, r)
				}
			}
			break
		}
	}
}
