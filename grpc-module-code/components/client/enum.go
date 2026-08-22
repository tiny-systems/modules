package client

import (
	"github.com/goccy/go-json"
	"github.com/swaggest/jsonschema-go"
)

// Enum special type which can carry its value and possible options for enum values
type Enum struct {
	Value   string
	Options []string
}

// MarshalJSON treat like underlying Value string
func (r Enum) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.Value)
}

// UnmarshalJSON treat like underlying Value string
func (r *Enum) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &r.Value)
}

func (r Enum) JSONSchema() (jsonschema.Schema, error) {
	name := jsonschema.Schema{}
	name.AddType(jsonschema.String)
	name.WithDefault(r.Value)
	name.WithExtraPropertiesItem("shared", true)
	enums := make([]interface{}, len(r.Options))
	for k, v := range r.Options {
		enums[k] = v
	}
	name.WithEnum(enums...)
	return name, nil
}
