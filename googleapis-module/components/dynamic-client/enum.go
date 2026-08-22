package dynamicclient

import (
	"github.com/goccy/go-json"
	"github.com/swaggest/jsonschema-go"
)

// Enum is a special type which carries both current value and available options
type Enum struct {
	Value   string
	Options []string
	Labels  []string // Human-readable labels for each option
}

// MarshalJSON serializes only the Value field
func (r Enum) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.Value)
}

// UnmarshalJSON deserializes only to the Value field
func (r *Enum) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &r.Value)
}

// JSONSchema generates a JSON schema with enum options
func (r Enum) JSONSchema() (jsonschema.Schema, error) {
	schema := jsonschema.Schema{}
	schema.AddType(jsonschema.String)
	schema.WithDefault(r.Value)
	schema.WithExtraPropertiesItem("shared", true)

	if len(r.Options) > 0 {
		enums := make([]interface{}, len(r.Options))
		for k, v := range r.Options {
			enums[k] = v
		}
		schema.WithEnum(enums...)

		if len(r.Labels) > 0 {
			schema.WithExtraPropertiesItem("enumTitles", r.Labels)
		}
	}

	return schema, nil
}

// ServiceName represents a Google API service selection
type ServiceName struct {
	Enum
}

func (s ServiceName) MarshalJSON() ([]byte, error) {
	return s.Enum.MarshalJSON()
}

func (s *ServiceName) UnmarshalJSON(data []byte) error {
	return s.Enum.UnmarshalJSON(data)
}

func (s ServiceName) JSONSchema() (jsonschema.Schema, error) {
	return s.Enum.JSONSchema()
}

// MethodName represents a Google API method selection
type MethodName struct {
	Enum
}

func (m MethodName) MarshalJSON() ([]byte, error) {
	return m.Enum.MarshalJSON()
}

func (m *MethodName) UnmarshalJSON(data []byte) error {
	return m.Enum.UnmarshalJSON(data)
}

func (m MethodName) JSONSchema() (jsonschema.Schema, error) {
	return m.Enum.JSONSchema()
}

// Interface compliance
var _ jsonschema.Exposer = (*Enum)(nil)
var _ jsonschema.Exposer = (*ServiceName)(nil)
var _ jsonschema.Exposer = (*MethodName)(nil)
var _ json.Marshaler = (*ServiceName)(nil)
var _ json.Unmarshaler = (*ServiceName)(nil)
var _ json.Marshaler = (*MethodName)(nil)
var _ json.Unmarshaler = (*MethodName)(nil)
