package plugin

import (
	"strings"
	"unicode"
)

// Entities reach plugins through operations named by convention, like the
// recipe conventions of the prototype: get(ref) reads with get-<entity>,
// propose-change(ref, patch) writes with update-<entity>, both on a
// connection that declares the entity (SalesOrder: get-sales-order,
// update-sales-order).

// ReadOperation names the operation that serves get for an entity.
func ReadOperation(entity string) string { return "get-" + kebab(entity) }

// ProposeOperation names the operation that applies proposals to an entity.
func ProposeOperation(entity string) string { return "update-" + kebab(entity) }

// ModelEvent names the lifecycle event a write operation causes:
// model.<Entity>.created for create-*, add-*, insert-* and record-*,
// deleted for delete-* and remove-*, updated otherwise.
func ModelEvent(entity, operation string) string {
	verb, _, _ := strings.Cut(operation, "-")
	switch verb {
	case "create", "add", "insert", "record":
		return "model." + entity + ".created"
	case "delete", "remove":
		return "model." + entity + ".deleted"
	}
	return "model." + entity + ".updated"
}

func kebab(s string) string {
	var b strings.Builder
	for i, r := range s {
		if unicode.IsUpper(r) {
			if i > 0 {
				b.WriteByte('-')
			}
			r = unicode.ToLower(r)
		}
		b.WriteRune(r)
	}
	return b.String()
}

// Camel converts a snake_case column to a payload field: external_id ->
// externalId.
func Camel(s string) string {
	parts := strings.Split(s, "_")
	for i := 1; i < len(parts); i++ {
		if parts[i] != "" {
			parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}
	return strings.Join(parts, "")
}
