package encode

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
	ComponentName = "jwt_encode"
	RequestPort   = "request"
	ResponsePort  = "response"
	ErrorPort     = "error"
)

type Context any

type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" required:"true" title:"Enable Error Port" description:"If error happen, error port will emit an error message"`
}

type Error struct {
	Context Context `json:"context"`
	Error   string  `json:"error"`
}

// SigningMethod special type which can carry its value and possible options for SigningMethods
type SigningMethod struct {
	Value   string
	Options []string
}

// MarshalJSON treat like underlying Value string
func (r *SigningMethod) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.Value)
}

// UnmarshalJSON treat like underlying Value string
func (r *SigningMethod) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &r.Value)
}

func (r *SigningMethod) JSONSchema() (jsonschema.Schema, error) {
	name := jsonschema.Schema{}
	name.AddType(jsonschema.String)
	name.WithTitle("Signing Method")
	name.WithDefault(r.Value)
	enums := make([]interface{}, len(r.Options))
	for k, v := range r.Options {
		enums[k] = v
	}
	name.WithEnum(enums...)
	return name, nil
}

type Request struct {
	Context       Context       `json:"context" configurable:"true" title:"Context" description:"Arbitrary message to be send alongside with encoded message"`
	SigningMethod SigningMethod `json:"signingMethod" required:"true" title:"Signing Method" description:""`
	Claims        MapClaims     `json:"claims" configurable:"true" required:"true" title:"Claims" description:""`
	Key           string        `json:"key" required:"true" format:"textarea" title:"Private Key" description:"Plain text or PEM formatted private key"`
}

type Response struct {
	Context Context `json:"context"`
	Token   string  `json:"token"`
}

type Component struct {
	settings Settings
}

func (h *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "JWT Encoder",
		Info:        "Generates JWT token",
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

	var (
		key    interface{}
		method jwt.SigningMethod = jwt.SigningMethodHS256
		err    error
	)

	switch in.SigningMethod.Value {
	case "ES256":
		method = jwt.SigningMethodES256
		key, err = jwt.ParseECPrivateKeyFromPEM([]byte(in.Key))
	case "ES384":
		method = jwt.SigningMethodES384
		key, err = jwt.ParseECPrivateKeyFromPEM([]byte(in.Key))
	case "ES512":
		method = jwt.SigningMethodES512
		key, err = jwt.ParseECPrivateKeyFromPEM([]byte(in.Key))
	case "HS256":
		method = jwt.SigningMethodHS256
		key = []byte(in.Key)
	case "HS384":
		method = jwt.SigningMethodHS384
		key = []byte(in.Key)
	case "HS512":
		method = jwt.SigningMethodHS512
		key = []byte(in.Key)
	case "RS256":
		method = jwt.SigningMethodRS256
		key, err = jwt.ParseRSAPrivateKeyFromPEM([]byte(in.Key))
	case "RS384":
		method = jwt.SigningMethodRS384
		key, err = jwt.ParseRSAPrivateKeyFromPEM([]byte(in.Key))
	case "RS512":
		method = jwt.SigningMethodRS512
		key, err = jwt.ParseRSAPrivateKeyFromPEM([]byte(in.Key))
	case "None":
		method = jwt.SigningMethodNone
	}

	token, err := jwt.NewWithClaims(method, in.Claims).SignedString(key)
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
		Token:   token,
		Context: in.Context,
	})
}

func (h *Component) Ports() []module.Port {
	ports := []module.Port{
		{
			Name:     RequestPort,
			Label:    "In",
			Position: module.Left,
			Configuration: Request{

				SigningMethod: SigningMethod{
					Value: "HS256", // default value
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
