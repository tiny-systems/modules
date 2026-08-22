package dynamicclient

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/rs/zerolog/log"
	"github.com/tiny-systems/googleapis-module/components/etc"
	"github.com/tiny-systems/googleapis-module/pkg/discovery"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "google_api_call"
	RequestPort   = "request"
	ResponsePort  = "response"
	ErrorPort     = "error"
)

// Settings holds the component configuration
type Settings struct {
	Service         ServiceName `json:"service" title:"Service" description:"Select a Google API service then save settings" tab:"API Selection"`
	Method          MethodName  `json:"method" title:"Method" description:"Select an API method" tab:"API Selection"`
	EnableErrorPort bool        `json:"enableErrorPort" required:"true" title:"Enable Error Port" tab:"General" description:"If request fails, error port will emit an error message"`
}

// Token represents an OAuth2 access token
type Token struct {
	AccessToken  string    `json:"accessToken" required:"true" title:"Access Token" description:"OAuth2 access token" configurable:"true"`
	TokenType    string    `json:"tokenType,omitempty" title:"Token Type"`
	RefreshToken string    `json:"refreshToken,omitempty" title:"Refresh Token"`
	Expiry       time.Time `json:"expiry,omitempty" title:"Expiry"`
}

// RequestParams wraps DynamicSchema for request parameters
type RequestParams struct {
	DynamicSchema
}

// ResponseBody wraps DynamicSchema for response body
type ResponseBody struct {
	DynamicSchema
}

// Request represents the input to the component
type Request struct {
	Context    any           `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	Token      Token         `json:"token" required:"true" title:"Token" description:"OAuth2 token for authentication"`
	Parameters RequestParams `json:"parameters" configurable:"true" title:"Parameters" description:"Request parameters based on selected API method"`
}

// Response represents the successful output
type Response struct {
	Context    any            `json:"context,omitempty" title:"Context"`
	StatusCode int            `json:"statusCode" title:"Status Code"`
	Headers    map[string]any `json:"headers,omitempty" title:"Response Headers"`
	Body       ResponseBody   `json:"body" title:"Response Body" description:"Response data based on selected API method"`
}

// Error represents an error output
type Error struct {
	Context any    `json:"context,omitempty" title:"Context"`
	Error   string `json:"error" title:"Error Message"`
	Code    int    `json:"code,omitempty" title:"Error Code"`
}

// Component implements the Google API client
type Component struct {
	settings     Settings
	settingsLock sync.RWMutex

	// Discovery client
	discoveryClient *discovery.Client

	// Cached API data
	currentAPI     *discovery.ServiceOption
	currentAPISpec interface{} // Will be *googleapisnewmodule.API when loaded
	currentMethod  interface{} // Will be *googleapisnewmodule.MethodInfo when loaded

	// Available options
	servicesAvailable []string
	servicesLabels    []string
	methodsAvailable  []string
	methodsLabels     []string

	// Request/Response schemas (dynamic)
	requestSchema  DynamicSchema
	responseSchema DynamicSchema
}

// Instance creates a new component instance
func (c *Component) Instance() module.Component {
	return &Component{
		settings: Settings{
			Service: ServiceName{Enum{Value: "", Options: []string{}, Labels: []string{}}},
			Method:  MethodName{Enum{Value: "", Options: []string{}, Labels: []string{}}},
		},
		discoveryClient:   discovery.NewClient(),
		servicesAvailable: []string{},
		servicesLabels:    []string{},
		methodsAvailable:  []string{},
		methodsLabels:     []string{},
	}
}

// GetInfo returns component metadata
func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Google API Client",
		Info:        "Universal Google API client. Select any Google API service and method from the discovery directory. Dynamically generates request/response schemas based on the official API specification. Supports all 400+ Google APIs.",
		Tags:        []string{"Google", "API", "REST", "Universal"},
	}
}

// Handle processes incoming messages on ports
// OnSettings stores the component settings.
func (c *Component) OnSettings(ctx context.Context, msg any) error {

	return c.handleSettings(ctx, msg)
}

// Handle dispatches the RequestPort. System ports go through capabilities.
func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}

	return c.handleRequest(ctx, handler, msg)
}

// handleSettings processes settings updates
func (c *Component) handleSettings(ctx context.Context, msg interface{}) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings message")
	}

	c.settingsLock.Lock()
	defer c.settingsLock.Unlock()

	// Discover available services if not loaded
	if len(c.servicesAvailable) == 0 {
		if err := c.discoverServices(ctx); err != nil {
			log.Warn().Err(err).Msg("failed to discover services")
		}
	}

	// Check if service changed - save old value before updating
	oldServiceValue := c.settings.Service.Value
	serviceChanged := oldServiceValue != in.Service.Value

	log.Info().
		Str("currentService", oldServiceValue).
		Str("newService", in.Service.Value).
		Str("inMethod", in.Method.Value).
		Bool("serviceChanged", serviceChanged).
		Msg("handleSettings: checking service change")

	// Update service selection
	c.settings.Service.Value = in.Service.Value
	c.settings.Service.Options = c.servicesAvailable
	c.settings.Service.Labels = c.servicesLabels

	// If service selected, discover methods for that service
	if in.Service.Value != "" {
		// Only re-discover if service changed or methods not loaded
		if serviceChanged || len(c.methodsAvailable) == 0 {
			// Clear previous methods
			c.methodsAvailable = []string{}
			c.methodsLabels = []string{}

			if err := c.discoverMethods(ctx, in.Service.Value); err != nil {
				log.Warn().Err(err).Msg("failed to discover methods")
			}
		}
	} else {
		// No service selected, clear methods
		c.methodsAvailable = []string{}
		c.methodsLabels = []string{}
	}

	// Update method selection - only reset if user actually changed service (not init from empty)
	if serviceChanged && oldServiceValue != "" {
		c.settings.Method.Value = "" // Reset method when user changes service
	} else {
		c.settings.Method.Value = in.Method.Value
	}
	c.settings.Method.Options = c.methodsAvailable
	c.settings.Method.Labels = c.methodsLabels

	// Update other settings
	c.settings.EnableErrorPort = in.EnableErrorPort

	// If method selected, build dynamic schemas
	// Use in.Method.Value since c.settings.Method.Value may have been reset
	methodToUse := c.settings.Method.Value
	if methodToUse == "" {
		methodToUse = in.Method.Value
	}

	if in.Service.Value != "" && methodToUse != "" {
		log.Info().
			Str("service", in.Service.Value).
			Str("method", methodToUse).
			Msg("building schemas for method")

		if err := c.buildSchemas(ctx, in.Service.Value, methodToUse); err != nil {
			log.Warn().Err(err).Msg("failed to build schemas")
		} else {
			log.Info().
				Bool("hasRequestSchema", c.requestSchema.schemaData != nil).
				Bool("hasResponseSchema", c.responseSchema.schemaData != nil).
				Msg("schemas built successfully")
		}
	}

	return nil
}

// handleRequest executes the API request
func (c *Component) handleRequest(ctx context.Context, handler module.Handler, msg interface{}) module.Result {
	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid request message"))
	}

	c.settingsLock.RLock()
	serviceID := c.settings.Service.Value
	methodName := c.settings.Method.Value
	enableErrorPort := c.settings.EnableErrorPort
	c.settingsLock.RUnlock()

	if serviceID == "" || methodName == "" {
		err := fmt.Errorf("service and method must be selected in settings")
		if enableErrorPort {
			return handler(ctx, ErrorPort, Error{
				Context: in.Context,
				Error:   err.Error(),
			})
		}
		return module.Fail(err)
	}

	// Execute the request
	response, err := c.executeRequest(ctx, serviceID, methodName, in)
	if err != nil {
		if enableErrorPort {
			return handler(ctx, ErrorPort, Error{
				Context: in.Context,
				Error:   err.Error(),
			})
		}
		return module.Fail(err)
	}

	response.Context = in.Context
	return handler(ctx, ResponsePort, response)
}

// discoverServices loads available Google API services
func (c *Component) discoverServices(ctx context.Context) error {
	services, err := c.discoveryClient.GetPreferredServices(ctx)
	if err != nil {
		return err
	}

	c.servicesAvailable = make([]string, len(services))
	c.servicesLabels = make([]string, len(services))

	for i, svc := range services {
		c.servicesAvailable[i] = svc.ID
		c.servicesLabels[i] = svc.Title
	}

	return nil
}

// discoverMethods loads available methods for a service
func (c *Component) discoverMethods(ctx context.Context, serviceID string) error {
	log.Info().Str("serviceID", serviceID).Msg("discovering methods for service")

	methods, err := c.discoveryClient.GetMethods(ctx, serviceID)
	if err != nil {
		log.Error().Err(err).Str("serviceID", serviceID).Msg("failed to get methods")
		return err
	}

	log.Info().
		Str("serviceID", serviceID).
		Int("numMethods", len(methods)).
		Msg("methods discovered for service")

	c.methodsAvailable = make([]string, len(methods))
	c.methodsLabels = make([]string, len(methods))

	for i, m := range methods {
		c.methodsAvailable[i] = m.FullName
		// Create label from method info
		label := m.FullName
		if m.Method.Description != "" {
			// Truncate long descriptions
			desc := m.Method.Description
			if len(desc) > 80 {
				desc = desc[:77] + "..."
			}
			label = fmt.Sprintf("%s - %s", m.FullName, desc)
		}
		c.methodsLabels[i] = label
	}

	return nil
}

// buildSchemas creates dynamic request/response schemas for the selected method
func (c *Component) buildSchemas(ctx context.Context, serviceID, methodName string) error {
	api, err := c.discoveryClient.GetAPI(ctx, serviceID)
	if err != nil {
		return err
	}

	log.Info().
		Str("apiName", api.Name).
		Str("apiTitle", api.Title).
		Int("numSchemas", len(api.Schemas)).
		Msg("API loaded")

	methods := api.GetAllMethods()
	log.Info().Int("numMethods", len(methods)).Msg("methods discovered")

	for _, m := range methods {
		if m.FullName == methodName {
			var requestRef, responseRef string
			if m.Method.Request != nil {
				requestRef = m.Method.Request.Ref
			}
			if m.Method.Response != nil {
				responseRef = m.Method.Response.Ref
			}
			log.Info().
				Str("method", m.FullName).
				Str("httpMethod", m.Method.HttpMethod).
				Int("numParams", len(m.Method.Parameters)).
				Str("requestRef", requestRef).
				Str("responseRef", responseRef).
				Msg("found method, building schemas")

			converter := NewSchemaConverter(api)
			c.requestSchema = converter.BuildRequestSchema(m.Method)
			c.responseSchema = converter.BuildResponseSchema(m.Method)
			c.currentMethod = &m

			// Log schema properties to debug
			var reqProps, respProps []string
			if c.requestSchema.schemaData != nil && c.requestSchema.schemaData.Properties != nil {
				for k := range c.requestSchema.schemaData.Properties {
					reqProps = append(reqProps, k)
				}
			}
			if c.responseSchema.schemaData != nil && c.responseSchema.schemaData.Properties != nil {
				for k := range c.responseSchema.schemaData.Properties {
					respProps = append(respProps, k)
				}
			}
			log.Info().
				Bool("requestHasSchema", c.requestSchema.schemaData != nil).
				Bool("responseHasSchema", c.responseSchema.schemaData != nil).
				Strs("requestSchemaProps", reqProps).
				Strs("responseSchemaProps", respProps).
				Msg("schemas built")

			return nil
		}
	}

	return fmt.Errorf("method %s not found", methodName)
}

// executeRequest makes the actual HTTP request to the Google API
func (c *Component) executeRequest(ctx context.Context, serviceID, methodName string, req Request) (*Response, error) {
	api, err := c.discoveryClient.GetAPI(ctx, serviceID)
	if err != nil {
		return nil, etc.ClassifyGoogleErr(fmt.Errorf("failed to get API spec: %w", err))
	}

	// Find the method
	methods := api.GetAllMethods()
	var methodInfo *struct {
		FullName string
		Method   interface{}
		Resource string
	}

	for _, m := range methods {
		if m.FullName == methodName {
			methodInfo = &struct {
				FullName string
				Method   interface{}
				Resource string
			}{m.FullName, m.Method, m.Resource}
			break
		}
	}

	if methodInfo == nil {
		return nil, fmt.Errorf("method %s not found", methodName)
	}

	// Get method details - need to re-fetch to get typed access
	var foundMethod interface{}
	for _, m := range methods {
		if m.FullName == methodName {
			foundMethod = m.Method
			break
		}
	}

	// Build the request URL
	baseURL := api.BaseUrl
	if baseURL == "" {
		baseURL = api.RootUrl + api.ServicePath
	}

	// Get method path and parameters
	methodData := methods[0].Method // We need the actual method
	for _, m := range methods {
		if m.FullName == methodName {
			methodData = m.Method
			break
		}
	}

	// Use flatPath if available, otherwise path
	path := methodData.FlatPath
	if path == "" {
		path = methodData.Path
	}

	// Build query parameters and substitute path parameters
	queryParams := url.Values{}
	pathParams := make(map[string]string)

	if req.Parameters.Data != nil {
		for name, value := range req.Parameters.Data {
			strValue := fmt.Sprintf("%v", value)
			param, hasParam := methodData.Parameters[name]

			if hasParam && param.Location == "path" {
				pathParams[name] = strValue
			} else if hasParam && param.Location == "query" {
				queryParams.Set(name, strValue)
			} else {
				// Unknown parameter, add to query (for body fields we'll handle separately)
				queryParams.Set(name, strValue)
			}
		}
	}

	// Substitute path parameters
	for name, value := range pathParams {
		path = strings.ReplaceAll(path, "{"+name+"}", url.PathEscape(value))
		path = strings.ReplaceAll(path, "{+"+name+"}", value) // For path-style params
	}

	// Build full URL
	fullURL := baseURL + path
	if len(queryParams) > 0 {
		fullURL += "?" + queryParams.Encode()
	}

	// Prepare request body for POST/PUT/PATCH
	var bodyReader io.Reader
	httpMethod := methodData.HttpMethod
	if httpMethod == "POST" || httpMethod == "PUT" || httpMethod == "PATCH" {
		// Build body from parameters that aren't path/query
		bodyData := make(map[string]any)
		if req.Parameters.Data != nil {
			for name, value := range req.Parameters.Data {
				param, hasParam := methodData.Parameters[name]
				if !hasParam || (param.Location != "path" && param.Location != "query") {
					bodyData[name] = value
				}
			}
		}

		if len(bodyData) > 0 {
			jsonBody, err := json.Marshal(bodyData)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal request body: %w", err)
			}
			bodyReader = bytes.NewReader(jsonBody)
		}
	}

	// Create HTTP request
	httpReq, err := http.NewRequestWithContext(ctx, httpMethod, fullURL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	httpReq.Header.Set("Authorization", fmt.Sprintf("Bearer %s", req.Token.AccessToken))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	// Execute request
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, etc.ClassifyGoogleErr(fmt.Errorf("request failed: %w", err))
	}
	defer resp.Body.Close()

	// Read response body
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, etc.ClassifyGoogleErr(fmt.Errorf("failed to read response: %w", err))
	}

	// Parse response
	var bodyData any
	if len(respBody) > 0 {
		if err := json.Unmarshal(respBody, &bodyData); err != nil {
			// If not JSON, return as string
			bodyData = string(respBody)
		}
	}

	// Convert headers
	headers := make(map[string]any)
	for k, v := range resp.Header {
		if len(v) == 1 {
			headers[k] = v[0]
		} else {
			headers[k] = v
		}
	}

	// Check for error status
	if resp.StatusCode >= 400 {
		apiErr := fmt.Errorf("API error %d: %v", resp.StatusCode, bodyData)
		if etc.RetryableHTTPStatus(resp.StatusCode) {
			apiErr = module.Retryable(apiErr)
		}
		return nil, apiErr
	}

	_ = foundMethod // silence unused warning

	// Convert body to ResponseBody
	var responseBody ResponseBody
	if bodyMap, ok := bodyData.(map[string]any); ok {
		responseBody = ResponseBody{DynamicSchema{Data: bodyMap}}
	} else {
		// Wrap non-object responses
		responseBody = ResponseBody{DynamicSchema{Data: map[string]any{"data": bodyData}}}
	}

	return &Response{
		StatusCode: resp.StatusCode,
		Headers:    headers,
		Body:       responseBody,
	}, nil
}

// Ports returns the component's port configuration
func (c *Component) Ports() []module.Port {
	c.settingsLock.RLock()
	defer c.settingsLock.RUnlock()

	log.Info().
		Bool("hasRequestSchema", c.requestSchema.schemaData != nil).
		Bool("hasResponseSchema", c.responseSchema.schemaData != nil).
		Interface("requestDataKeys", getMapKeys(c.requestSchema.Data)).
		Interface("responseDataKeys", getMapKeys(c.responseSchema.Data)).
		Msg("Ports: returning port configuration")

	// Build settings with current options for dropdowns
	settings := Settings{
		Service: ServiceName{
			Enum: Enum{
				Value:   c.settings.Service.Value,
				Options: c.servicesAvailable,
				Labels:  c.servicesLabels,
			},
		},
		Method: MethodName{
			Enum: Enum{
				Value:   c.settings.Method.Value,
				Options: c.methodsAvailable,
				Labels:  c.methodsLabels,
			},
		},
		EnableErrorPort: c.settings.EnableErrorPort,
	}

	ports := []module.Port{
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: settings,
		},
		{
			Name:     RequestPort,
			Label:    "Request",
			Position: module.Left,
			Configuration: Request{
				Parameters: RequestParams{c.requestSchema},
			},
		},
		{
			Name:     ResponsePort,
			Label:    "Response",
			Position: module.Right,
			Source:   true,
			Configuration: Response{
				Body: ResponseBody{c.responseSchema},
			},
		},
	}

	if c.settings.EnableErrorPort {
		ports = append(ports, module.Port{
			Name:          ErrorPort,
			Label:         "Error",
			Position:      module.Bottom,
			Source:        true,
			Configuration: Error{},
		})
	}

	return ports
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
)

// getMapKeys returns keys from a map for logging
func getMapKeys(m map[string]any) []string {
	if m == nil {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func init() {
	registry.Register((&Component{}).Instance())
}
