package client

import (
	"github.com/goccy/go-json"
	"github.com/swaggest/jsonschema-go"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// MessageDescriptor builds schema based on descriptor
// returns marshalled output data raw
// marshals data to input
type MessageDescriptor struct {
	Input      any
	Output     []byte
	Descriptor protoreflect.MessageDescriptor `json:"-"`
}

// MarshalJSON treat like underlying Value string
func (r MessageDescriptor) MarshalJSON() ([]byte, error) {
	var output = r.Output
	if len(output) == 0 {
		output = []byte("{}")
	}
	return output, nil
}

// UnmarshalJSON treat like underlying Value string
func (r *MessageDescriptor) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &r.Input)
}

func (r MessageDescriptor) JSONSchema() (jsonschema.Schema, error) {
	schema := &jsonschema.Schema{}
	schema.AddType(jsonschema.Object)
	schema.WithDefault(r.Input)
	schema.WithExtraPropertiesItem("shared", true)
	r.messageToSchema(schema, r.Descriptor)
	return *schema, nil
}

func (r MessageDescriptor) messageToSchema(schema *jsonschema.Schema, msgDescriptor protoreflect.MessageDescriptor) {

	if msgDescriptor == nil {
		return
	}
	fieldsDescriptors := msgDescriptor.Fields()
	if fieldsDescriptors == nil {
		return
	}
	for i := 0; i < fieldsDescriptors.Len(); i++ {
		field := fieldsDescriptors.Get(i)

		typ := jsonschema.Null.Type()

		propSchema := &jsonschema.Schema{}
		switch field.Kind() {

		case protoreflect.BoolKind:
			typ = jsonschema.Boolean.Type()
		case protoreflect.StringKind:
			typ = jsonschema.String.Type()
		case protoreflect.Int32Kind, protoreflect.Int64Kind, protoreflect.Uint64Kind, protoreflect.Uint32Kind:
			typ = jsonschema.Integer.Type()
		case protoreflect.FloatKind, protoreflect.DoubleKind:
			typ = jsonschema.Number.Type()
		case protoreflect.EnumKind:
			// @todo add enum support

		case protoreflect.MessageKind:
			typ = jsonschema.Object.Type()
			r.messageToSchema(propSchema, field.Message())
		}
		schema.WithPropertiesItem(field.JSONName(), propSchema.WithType(typ).ToSchemaOrBool())
	}
}
