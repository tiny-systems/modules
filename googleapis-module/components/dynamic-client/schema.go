package dynamicclient

import (
	"github.com/goccy/go-json"
	"github.com/swaggest/jsonschema-go"
	googleapismodule "github.com/tiny-systems/googleapis-module"
)

// DynamicSchema wraps a dynamically generated schema from Google's discovery format
type DynamicSchema struct {
	Data       map[string]any
	schemaData *jsonschema.Schema
}

// MarshalJSON serializes the Data map
func (d DynamicSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Data)
}

// UnmarshalJSON deserializes to the Data map
func (d *DynamicSchema) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &d.Data)
}

// JSONSchema returns the pre-computed schema
func (d DynamicSchema) JSONSchema() (jsonschema.Schema, error) {
	if d.schemaData != nil {
		return *d.schemaData, nil
	}
	// Return empty object schema if no schema data
	schema := jsonschema.Schema{}
	schema.AddType(jsonschema.Object)
	return schema, nil
}

var _ jsonschema.Exposer = (*DynamicSchema)(nil)
var _ jsonschema.Exposer = (*RequestParams)(nil)
var _ jsonschema.Exposer = (*ResponseBody)(nil)

// SchemaConverter converts Google Discovery schemas to JSON schemas
type SchemaConverter struct {
	api      *googleapismodule.API
	maxDepth int
	visited  map[string]bool // Track visited refs to prevent infinite recursion
}

// NewSchemaConverter creates a new converter for an API
func NewSchemaConverter(api *googleapismodule.API) *SchemaConverter {
	return &SchemaConverter{
		api:      api,
		maxDepth: 10,
		visited:  make(map[string]bool),
	}
}

// BuildRequestSchema creates a DynamicSchema for a method's request
func (c *SchemaConverter) BuildRequestSchema(method googleapismodule.Method) DynamicSchema {
	schema := &jsonschema.Schema{}
	schema.AddType(jsonschema.Object)
	schema.WithExtraPropertiesItem("configurable", true)

	properties := make(map[string]jsonschema.SchemaOrBool)
	required := make([]string, 0)
	sampleData := make(map[string]any)

	// Add path and query parameters
	for name, param := range method.Parameters {
		propSchema := c.parameterToSchema(param)
		properties[name] = jsonschema.SchemaOrBool{TypeObject: propSchema}
		sampleData[name] = nil // Will be filled by user

		if param.Required {
			required = append(required, name)
		}
	}

	// Add request body if present
	if method.Request != nil && method.Request.Ref != "" {
		sampleData["_requestBodyRef"] = method.Request.Ref
		if bodySchema, ok := c.api.Schemas[method.Request.Ref]; ok {
			// Merge body properties into main schema
			c.visited = make(map[string]bool) // Reset visited for new conversion
			bodyJSONSchema := c.schemaToJSONSchema(bodySchema, 0)
			if bodyJSONSchema.Properties != nil {
				for name, prop := range bodyJSONSchema.Properties {
					properties[name] = prop
					sampleData[name] = nil
				}
			}
		}
	}

	if len(properties) > 0 {
		schema.WithProperties(properties)
	}
	if len(required) > 0 {
		schema.Required = required
	}

	return DynamicSchema{
		Data:       sampleData,
		schemaData: schema,
	}
}

// BuildResponseSchema creates a DynamicSchema for a method's response
func (c *SchemaConverter) BuildResponseSchema(method googleapismodule.Method) DynamicSchema {
	if method.Response == nil || method.Response.Ref == "" {
		// No response schema, return empty object
		schema := &jsonschema.Schema{}
		schema.AddType(jsonschema.Object)
		schema.WithDescription("No response schema defined for this method")
		return DynamicSchema{
			Data:       map[string]any{"_info": "No response schema defined"},
			schemaData: schema,
		}
	}

	responseSchema, ok := c.api.Schemas[method.Response.Ref]
	if !ok {
		schema := &jsonschema.Schema{}
		schema.AddType(jsonschema.Object)
		schema.WithDescription("Response schema not found: " + method.Response.Ref)
		return DynamicSchema{
			Data:       map[string]any{"_error": "Schema not found: " + method.Response.Ref},
			schemaData: schema,
		}
	}

	// Build schema explicitly like we do for request
	schema := &jsonschema.Schema{}
	schema.AddType(jsonschema.Object)
	if responseSchema.Description != "" {
		schema.WithDescription(responseSchema.Description)
	}

	properties := make(map[string]jsonschema.SchemaOrBool)
	sampleData := make(map[string]any)

	// Convert each property from the response schema
	c.visited = make(map[string]bool)
	if responseSchema.Properties != nil {
		for name, prop := range responseSchema.Properties {
			propSchema := c.schemaToJSONSchema(prop, 0)
			properties[name] = jsonschema.SchemaOrBool{TypeObject: propSchema}
			sampleData[name] = nil
		}
	}

	if len(properties) > 0 {
		schema.WithProperties(properties)
	}

	return DynamicSchema{
		Data:       sampleData,
		schemaData: schema,
	}
}

// parameterToSchema converts a Google API parameter to JSON schema
func (c *SchemaConverter) parameterToSchema(param googleapismodule.Parameter) *jsonschema.Schema {
	schema := &jsonschema.Schema{}

	// Set type
	switch param.Type {
	case "string":
		schema.AddType(jsonschema.String)
	case "integer":
		schema.AddType(jsonschema.Integer)
	case "number":
		schema.AddType(jsonschema.Number)
	case "boolean":
		schema.AddType(jsonschema.Boolean)
	case "array":
		schema.AddType(jsonschema.Array)
		if param.Items != nil {
			itemSchema := c.parameterToSchema(*param.Items)
			schema.Items = &jsonschema.Items{SchemaOrBool: &jsonschema.SchemaOrBool{TypeObject: itemSchema}}
		}
	default:
		schema.AddType(jsonschema.String)
	}

	// Add description
	if param.Description != "" {
		schema.WithDescription(param.Description)
	}

	// Add default
	if param.Default != "" {
		schema.WithDefault(param.Default)
	}

	// Add enum
	if len(param.Enum) > 0 {
		enums := make([]interface{}, len(param.Enum))
		for i, v := range param.Enum {
			enums[i] = v
		}
		schema.WithEnum(enums...)

		if len(param.EnumDescriptions) > 0 {
			schema.WithExtraPropertiesItem("enumTitles", param.EnumDescriptions)
		}
	}

	// Add format
	if param.Format != "" {
		schema.WithFormat(param.Format)
	}

	// Add pattern
	if param.Pattern != "" {
		schema.WithPattern(param.Pattern)
	}

	// Mark as configurable for the module system
	schema.WithExtraPropertiesItem("configurable", true)

	return schema
}

// schemaToJSONSchema converts a Google Discovery schema to JSON schema
func (c *SchemaConverter) schemaToJSONSchema(gSchema googleapismodule.Schema, depth int) *jsonschema.Schema {
	if depth > c.maxDepth {
		// Prevent infinite recursion
		schema := &jsonschema.Schema{}
		schema.AddType(jsonschema.Object)
		return schema
	}

	schema := &jsonschema.Schema{}

	// Handle $ref
	if gSchema.Ref != "" {
		if c.visited[gSchema.Ref] {
			// Already visited, return generic object to prevent recursion
			schema.AddType(jsonschema.Object)
			return schema
		}
		c.visited[gSchema.Ref] = true

		if refSchema, ok := c.api.Schemas[gSchema.Ref]; ok {
			return c.schemaToJSONSchema(refSchema, depth+1)
		}
		schema.AddType(jsonschema.Object)
		return schema
	}

	// Set type
	switch gSchema.Type {
	case "object":
		schema.AddType(jsonschema.Object)
		if len(gSchema.Properties) > 0 {
			properties := make(map[string]jsonschema.SchemaOrBool)
			for name, prop := range gSchema.Properties {
				propSchema := c.schemaToJSONSchema(prop, depth+1)
				properties[name] = jsonschema.SchemaOrBool{TypeObject: propSchema}
			}
			schema.WithProperties(properties)
		}
		if gSchema.AdditionalProperties != nil {
			addSchema := c.schemaToJSONSchema(*gSchema.AdditionalProperties, depth+1)
			schema.WithAdditionalProperties(jsonschema.SchemaOrBool{TypeObject: addSchema})
		}
	case "array":
		schema.AddType(jsonschema.Array)
		if gSchema.Items != nil {
			itemSchema := c.schemaToJSONSchema(*gSchema.Items, depth+1)
			schema.Items = &jsonschema.Items{SchemaOrBool: &jsonschema.SchemaOrBool{TypeObject: itemSchema}}
		}
	case "string":
		schema.AddType(jsonschema.String)
	case "integer":
		schema.AddType(jsonschema.Integer)
	case "number":
		schema.AddType(jsonschema.Number)
	case "boolean":
		schema.AddType(jsonschema.Boolean)
	case "any":
		// Any type - leave schema open
	default:
		if gSchema.Type != "" {
			schema.AddType(jsonschema.String) // Fallback
		}
	}

	// Add description
	if gSchema.Description != "" {
		schema.WithDescription(gSchema.Description)
	}

	// Add default
	if gSchema.Default != nil {
		schema.WithDefault(gSchema.Default)
	}

	// Add enum
	if len(gSchema.Enum) > 0 {
		enums := make([]interface{}, len(gSchema.Enum))
		for i, v := range gSchema.Enum {
			enums[i] = v
		}
		schema.WithEnum(enums...)

		if len(gSchema.EnumDescriptions) > 0 {
			schema.WithExtraPropertiesItem("enumTitles", gSchema.EnumDescriptions)
		}
	}

	// Add format
	if gSchema.Format != "" {
		schema.WithFormat(gSchema.Format)
	}

	// Add pattern
	if gSchema.Pattern != "" {
		schema.WithPattern(gSchema.Pattern)
	}

	// Mark as configurable
	schema.WithExtraPropertiesItem("configurable", true)

	return schema
}
