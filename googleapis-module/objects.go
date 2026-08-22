package googleapismodule

// Discovery represents the Google API Discovery directory list
type Discovery struct {
	Kind             string          `json:"kind"`
	DiscoveryVersion string          `json:"discoveryVersion"`
	Items            []DiscoveryItem `json:"items"`
}

// DiscoveryItem represents a single API in the discovery directory
type DiscoveryItem struct {
	Kind             string `json:"kind"`
	ID               string `json:"id"`
	Name             string `json:"name"`
	Version          string `json:"version"`
	Title            string `json:"title"`
	Description      string `json:"description"`
	DiscoveryRestUrl string `json:"discoveryRestUrl"`
	Icons            struct {
		X16 string `json:"x16"`
		X32 string `json:"x32"`
	} `json:"icons"`
	DocumentationLink string `json:"documentationLink"`
	Preferred         bool   `json:"preferred"`
}

// API represents a full Google API specification from a discovery document
type API struct {
	Kind              string               `json:"kind"`
	ID                string               `json:"id"`
	Name              string               `json:"name"`
	Version           string               `json:"version"`
	Title             string               `json:"title"`
	Description       string               `json:"description,omitempty"`
	CanonicalName     string               `json:"canonicalName,omitempty"`
	DiscoveryVersion  string               `json:"discoveryVersion,omitempty"`
	DocumentationLink string               `json:"documentationLink,omitempty"`
	RootUrl           string               `json:"rootUrl,omitempty"`
	BasePath          string               `json:"basePath,omitempty"`
	BaseUrl           string               `json:"baseUrl,omitempty"`
	ServicePath       string               `json:"servicePath,omitempty"`
	Revision          string               `json:"revision,omitempty"`
	Protocol          string               `json:"protocol,omitempty"`
	BatchPath         string               `json:"batchPath,omitempty"`
	Parameters        map[string]Parameter `json:"parameters,omitempty"`
	Auth              *Auth                `json:"auth,omitempty"`
	Schemas           map[string]Schema    `json:"schemas,omitempty"`
	Resources         map[string]Resource  `json:"resources,omitempty"`
	Methods           map[string]Method    `json:"methods,omitempty"` // Top-level methods (rare)
}

// Auth represents authentication configuration
type Auth struct {
	OAuth2 *OAuth2Config `json:"oauth2,omitempty"`
}

// OAuth2Config represents OAuth2 scopes
type OAuth2Config struct {
	Scopes map[string]ScopeInfo `json:"scopes,omitempty"`
}

// ScopeInfo represents information about an OAuth2 scope
type ScopeInfo struct {
	Description string `json:"description"`
}

// Resource represents a REST resource with methods and nested resources
type Resource struct {
	Methods   map[string]Method   `json:"methods,omitempty"`
	Resources map[string]Resource `json:"resources,omitempty"`
}

// Method represents an API method/operation
type Method struct {
	ID                      string               `json:"id"`
	Path                    string               `json:"path"`
	FlatPath                string               `json:"flatPath,omitempty"`
	HttpMethod              string               `json:"httpMethod"`
	Description             string               `json:"description,omitempty"`
	Parameters              map[string]Parameter `json:"parameters,omitempty"`
	ParameterOrder          []string             `json:"parameterOrder,omitempty"`
	Request                 *SchemaRef           `json:"request,omitempty"`
	Response                *SchemaRef           `json:"response,omitempty"`
	Scopes                  []string             `json:"scopes,omitempty"`
	SupportsMediaDownload   bool                 `json:"supportsMediaDownload,omitempty"`
	SupportsMediaUpload     bool                 `json:"supportsMediaUpload,omitempty"`
	MediaUpload             *MediaUpload         `json:"mediaUpload,omitempty"`
	UseMediaDownloadService bool                 `json:"useMediaDownloadService,omitempty"`
}

// SchemaRef represents a reference to a schema
type SchemaRef struct {
	Ref           string `json:"$ref,omitempty"`
	ParameterName string `json:"parameterName,omitempty"`
}

// MediaUpload represents media upload configuration
type MediaUpload struct {
	Accept    []string            `json:"accept,omitempty"`
	MaxSize   string              `json:"maxSize,omitempty"`
	Protocols map[string]Protocol `json:"protocols,omitempty"`
}

// Protocol represents an upload protocol
type Protocol struct {
	Multipart bool   `json:"multipart,omitempty"`
	Path      string `json:"path,omitempty"`
}

// Parameter represents a method parameter
type Parameter struct {
	Type             string   `json:"type,omitempty"`
	Description      string   `json:"description,omitempty"`
	Location         string   `json:"location,omitempty"` // "path", "query"
	Required         bool     `json:"required,omitempty"`
	Repeated         bool     `json:"repeated,omitempty"`
	Default          string   `json:"default,omitempty"`
	Minimum          string   `json:"minimum,omitempty"`
	Maximum          string   `json:"maximum,omitempty"`
	Enum             []string `json:"enum,omitempty"`
	EnumDescriptions []string `json:"enumDescriptions,omitempty"`
	Format           string   `json:"format,omitempty"` // "int32", "int64", "uint32", "uint64", "double", "float", "byte", "date", "date-time", "google-datetime", "google-duration", "google-fieldmask"
	Pattern          string   `json:"pattern,omitempty"`
	// For array/repeated parameters
	Items *Parameter `json:"items,omitempty"`
}

// Schema represents a data type schema in Google's discovery format
type Schema struct {
	ID               string   `json:"id,omitempty"`
	Type             string   `json:"type,omitempty"` // "object", "array", "string", "integer", "number", "boolean", "any"
	Ref              string   `json:"$ref,omitempty"`
	Description      string   `json:"description,omitempty"`
	Default          any      `json:"default,omitempty"`
	Required         bool     `json:"required,omitempty"`
	Format           string   `json:"format,omitempty"`
	Enum             []string `json:"enum,omitempty"`
	EnumDescriptions []string `json:"enumDescriptions,omitempty"`
	Minimum          string   `json:"minimum,omitempty"`
	Maximum          string   `json:"maximum,omitempty"`
	Pattern          string   `json:"pattern,omitempty"`
	ReadOnly         bool     `json:"readOnly,omitempty"`
	// Object properties
	Properties           map[string]Schema `json:"properties,omitempty"`
	AdditionalProperties *Schema           `json:"additionalProperties,omitempty"`
	// Array items
	Items *Schema `json:"items,omitempty"`
	// Annotations for documentation
	Annotations *Annotations `json:"annotations,omitempty"`
}

// Annotations contains documentation annotations
type Annotations struct {
	Required []string `json:"required,omitempty"`
}

// MethodInfo is a flattened representation of a method with its full path
type MethodInfo struct {
	FullName string // e.g., "spreadsheets.values.get"
	Method   Method
	Resource string // e.g., "spreadsheets.values"
}

// GetAllMethods returns all methods from an API flattened with their full names
func (api *API) GetAllMethods() []MethodInfo {
	var methods []MethodInfo

	// Top-level methods (rare)
	for name, method := range api.Methods {
		methods = append(methods, MethodInfo{
			FullName: name,
			Method:   method,
			Resource: "",
		})
	}

	// Recursively get methods from resources
	for resourceName, resource := range api.Resources {
		methods = append(methods, getMethodsFromResource(resourceName, resource)...)
	}

	return methods
}

func getMethodsFromResource(prefix string, resource Resource) []MethodInfo {
	var methods []MethodInfo

	for methodName, method := range resource.Methods {
		methods = append(methods, MethodInfo{
			FullName: prefix + "." + methodName,
			Method:   method,
			Resource: prefix,
		})
	}

	for subResourceName, subResource := range resource.Resources {
		methods = append(methods, getMethodsFromResource(prefix+"."+subResourceName, subResource)...)
	}

	return methods
}
