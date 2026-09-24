package v1alpha1

import (
	"encoding/json"
	"fmt"
)

// Connection binds a connector to one concrete system in the customer's
// environment: the output of pipeline stage 1, "Connect" (architecture §6).
// Recipes may name a connection instead of a connector.
//
// Generic connectors such as JDBC databases cannot know a customer's tables
// in advance, so a connection may declare the entities, events and
// operations discovered for that system. They are merged into the
// connector's manifest and must still use interfaces the manifest permits:
// a connection can narrow what a connector does, never widen how it does it.
type Connection struct {
	TypeMeta
	Metadata ObjectMeta     `json:"metadata"`
	Spec     ConnectionSpec `json:"spec"`
}

type ConnectionSpec struct {
	// Connector is a connector reference, e.g. "postgres@^1".
	Connector string `json:"connector"`
	// SecretRef overrides the manifest's secret reference for this system.
	SecretRef  string           `json:"secretRef,omitempty"`
	Entities   []string         `json:"entities,omitempty"`
	Events     []ConnectorEvent `json:"events,omitempty"`
	Operations []Operation      `json:"operations,omitempty"`
	// Config is connector-specific configuration, passed to the worker as-is.
	Config json.RawMessage `json:"config,omitempty"`
}

func (c *Connection) GetTypeMeta() TypeMeta { return c.TypeMeta }
func (c *Connection) GetMeta() ObjectMeta   { return c.Metadata }

func (c *Connection) Validate() FieldErrors {
	var es FieldErrors
	validateTypeMeta(c.TypeMeta, KindConnection, &es)
	validateMeta(c.Metadata, false, &es)
	if name, _ := ParseRef(c.Spec.Connector); !nameRE.MatchString(name) {
		es.add("spec.connector", "must reference a connector by name, got %q", c.Spec.Connector)
	}
	if c.Spec.SecretRef != "" && !secretRefRE.MatchString(c.Spec.SecretRef) {
		es.add("spec.secretRef", "must be a secret-store reference such as openbao://path, got %q", c.Spec.SecretRef)
	}
	if len(c.Spec.Config) > 0 {
		var obj map[string]any
		if err := json.Unmarshal(c.Spec.Config, &obj); err != nil {
			es.add("spec.config", "must be an object")
		}
	}
	return es
}

// Effective returns the manifest as this connection sees it: the connector's
// own declarations plus the connection's, with the connection's secret
// reference. The result must be validated; Validate on it reports any
// connection operation that uses an interface the connector does not permit.
func (c *Connection) Effective(m *ConnectorManifest) *ConnectorManifest {
	e := *m
	e.Spec.Entities = append(append([]string{}, m.Spec.Entities...), c.Spec.Entities...)
	e.Spec.Events = append(append([]ConnectorEvent{}, m.Spec.Events...), c.Spec.Events...)
	e.Spec.Operations = append(append([]Operation{}, m.Spec.Operations...), c.Spec.Operations...)
	if c.Spec.SecretRef != "" {
		e.Spec.Auth.SecretRef = c.Spec.SecretRef
	}
	return &e
}

// ValidateEffective validates the merged manifest and prefixes paths so
// errors point at the connection.
func (c *Connection) ValidateEffective(m *ConnectorManifest) FieldErrors {
	var es FieldErrors
	for _, e := range c.Effective(m).Validate() {
		es = append(es, FieldError{Path: fmt.Sprintf("connection %s: %s", c.Metadata.Name, e.Path), Message: e.Message})
	}
	return es
}
