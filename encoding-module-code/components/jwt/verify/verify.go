package verify

import (
	"context"
	"fmt"

	"github.com/goccy/go-json"
	"github.com/golang-jwt/jwt/v5"
	"github.com/swaggest/jsonschema-go"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "jwt_decode"
	RequestPort   = "request"
	ResponsePort  = "response"
	ErrorPort     = "error"
)

type Context any

type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" required:"true" title:"Enable Error Port" description:"If error happens, error port will emit an error message"`
}

type Error struct {
	Context Context `json:"context"`
	Error   string  `json:"error"`
}

// SigningMethod carries value and possible options for verification algorithms.
type SigningMethod struct {
	Value   string
	Options []string
}

func (r *SigningMethod) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.Value)
}

func (r *SigningMethod) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &r.Value)
}

func (r *SigningMethod) JSONSchema() (jsonschema.Schema, error) {
	s := jsonschema.Schema{}
	s.AddType(jsonschema.String)
	s.WithTitle("Signing Method")
	s.WithDefault(r.Value)
	enums := make([]interface{}, len(r.Options))
	for k, v := range r.Options {
		enums[k] = v
	}
	s.WithEnum(enums...)
	return s, nil
}

type Request struct {
	Context       Context      `json:"context" configurable:"true" title:"Context" description:"Arbitrary message to pass through"`
	SigningMethod SigningMethod `json:"signingMethod" required:"true" title:"Signing Method"`
	Token         string       `json:"token" required:"true" title:"Token" description:"JWT token to verify and decode"`
	Key           string       `json:"key" required:"true" format:"textarea" title:"Key" description:"Plain text secret or PEM formatted public key"`
}

// Claims represents decoded JWT claims.
type Claims map[string]interface{}

func (m Claims) JSONSchema() (jsonschema.Schema, error) {
	s := jsonschema.Schema{}
	s.AddType(jsonschema.Object)
	s.WithProperties(map[string]jsonschema.SchemaOrBool{
		"sub": (&jsonschema.Schema{}).WithTitle("Subject").WithType(jsonschema.String.Type()).ToSchemaOrBool(),
		"iss": (&jsonschema.Schema{}).WithTitle("Issuer").WithType(jsonschema.String.Type()).ToSchemaOrBool(),
		"aud": (&jsonschema.Schema{}).WithTitle("Audience").WithType(jsonschema.Array.Type()).WithItems(*(&jsonschema.Items{}).WithSchemaOrBool((&jsonschema.Schema{}).WithType(jsonschema.String.Type()).ToSchemaOrBool())).ToSchemaOrBool(),
		"exp": (&jsonschema.Schema{}).WithTitle("ExpiresAt").WithType(jsonschema.Integer.Type()).ToSchemaOrBool(),
		"nbf": (&jsonschema.Schema{}).WithTitle("NotBefore").WithType(jsonschema.Integer.Type()).ToSchemaOrBool(),
		"iat": (&jsonschema.Schema{}).WithTitle("IssuedAt").WithType(jsonschema.Integer.Type()).ToSchemaOrBool(),
		"jti": (&jsonschema.Schema{}).WithTitle("ID").WithType(jsonschema.String.Type()).ToSchemaOrBool(),
	})
	return s, nil
}

type Response struct {
	Context Context `json:"context"`
	Claims  Claims  `json:"claims" configurable:"true" title:"Claims" description:"Decoded JWT claims"`
}

type Component struct {
	settings Settings
}

func (h *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "JWT Decoder",
		Info:        "Verifies and decodes JWT token",
		Tags:        []string{"jwt"},
	}
}

// OnSettings stores the component settings.
func (h *Component) OnSettings(_ context.Context, msg any) error {

	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	h.settings = in
	return nil
}

// Handle dispatches the RequestPort. System ports go through capabilities.
func (h *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}

	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid input"))
	}

	claims, err := parseToken(in.Token, in.Key, in.SigningMethod.Value)
	if err != nil {
		if !h.settings.EnableErrorPort {
			return module.Fail(err)
		}
		return handler(ctx, ErrorPort, Error{
			Context: in.Context,
			Error:   err.Error(),
		})
	}

	return handler(ctx, ResponsePort, Response{
		Context: in.Context,
		Claims:  Claims(claims),
	})
}

func parseToken(tokenString, key, method string) (jwt.MapClaims, error) {
	keyFunc, err := keyFuncForMethod(method, key)
	if err != nil {
		return nil, err
	}

	token, err := jwt.Parse(tokenString, keyFunc)
	if err != nil {
		return nil, fmt.Errorf("token verification failed: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("unexpected claims type")
	}

	return claims, nil
}

func keyFuncForMethod(method, key string) (jwt.Keyfunc, error) {
	switch method {
	case "ES256", "ES384", "ES512":
		pubKey, err := jwt.ParseECPublicKeyFromPEM([]byte(key))
		if err != nil {
			return nil, fmt.Errorf("parse EC public key: %w", err)
		}
		return func(*jwt.Token) (interface{}, error) { return pubKey, nil }, nil

	case "RS256", "RS384", "RS512":
		pubKey, err := jwt.ParseRSAPublicKeyFromPEM([]byte(key))
		if err != nil {
			return nil, fmt.Errorf("parse RSA public key: %w", err)
		}
		return func(*jwt.Token) (interface{}, error) { return pubKey, nil }, nil

	case "HS256", "HS384", "HS512":
		return func(*jwt.Token) (interface{}, error) { return []byte(key), nil }, nil

	case "None":
		return func(*jwt.Token) (interface{}, error) { return jwt.UnsafeAllowNoneSignatureType, nil }, nil

	default:
		return nil, fmt.Errorf("unsupported signing method: %s", method)
	}
}

func (h *Component) Ports() []module.Port {
	ports := []module.Port{
		{
			Name:     RequestPort,
			Label:    "In",
			Position: module.Left,
			Configuration: Request{
				SigningMethod: SigningMethod{
					Value: "HS256",
					Options: []string{
						"ES256", "ES384", "ES512", "HS256", "HS384", "HS512", "RS256", "RS384", "RS512", "None",
					},
				},
			},
		},
		{
			Name:          ResponsePort,
			Position:      module.Right,
			Label:         "Out",
			Source:        true,
			Configuration: Response{},
		},
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: h.settings,
		},
	}
	if !h.settings.EnableErrorPort {
		return ports
	}
	return append(ports, module.Port{
		Position:      module.Bottom,
		Name:          ErrorPort,
		Label:         "Error",
		Source:        true,
		Configuration: Error{},
	})
}

func (h *Component) Instance() module.Component {
	return &Component{
		settings: Settings{},
	}
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
)

func init() {
	registry.Register(&Component{})
}
