package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"github.com/fullstorydev/grpcurl"
	"github.com/goccy/go-json"
	"github.com/golang/protobuf/jsonpb"
	"github.com/jhump/protoreflect/v2/grpcdynamic"
	"github.com/jhump/protoreflect/v2/grpcreflect"
	"github.com/swaggest/jsonschema-go"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"sort"
	"strings"
	"time"
)

const (
	ComponentName = "grpc_call"
	RequestPort   = "request"
	ResponsePort  = "response"
	ErrorPort     = "error"
)

type Context any

type Settings struct {
	Address         string      `json:"address" title:"gRPC server address" required:"true" tab:"Connect"`
	Insecure        bool        `json:"insecure" title:"Insecure mode" default:"false" tab:"Connect" description:"Connect without TLS (plaintext). When off, TLS with system root certificates is used."`
	KeepAlive       bool        `json:"keepAlive" title:"Keep Alive" default:"false" tab:"Connect" description:"Send HTTP/2 keepalive pings (every 30s, 10s timeout) to hold long-lived connections open, even while idle."`
	Service         ServiceName `json:"service" title:"Service" description:"Name of the service" tab:"Request"`
	Method          MethodName  `json:"method" title:"Method" description:"Name of the gRPC method" tab:"Request"`
	EnableErrorPort bool        `json:"enableErrorPort" required:"true" title:"Enable Error Port" tab:"General" description:"If error happen, error port will emit an error message"`
}

// MethodName special type which can carry its value and possible options for enum values
type MethodName struct {
	Enum
}

// ServiceName special type which can carry its value and possible options for enum values
type ServiceName struct {
	Enum
}

type RequestMsg struct {
	MessageDescriptor
}

type ResponseMsg struct {
	MessageDescriptor
}

// Header is a single gRPC metadata key/value pair (same shape convention as
// http-module's etc.Header, defined locally to avoid a cross-module import).
type Header struct {
	Key   string `json:"key" required:"true" title:"Key" colSpan:"col-span-6"`
	Value string `json:"value" required:"true" title:"Value" colSpan:"col-span-6"`
}

type Request struct {
	Context     Context    `json:"context" configurable:"true" title:"Context" description:"Arbitrary message to be send alongside with encoded message"`
	BearerToken string     `json:"bearerToken,omitempty" format:"password" title:"Bearer Token" description:"Sent as 'authorization: Bearer <token>' call metadata. An explicit authorization header below overrides it."`
	Headers     []Header   `json:"headers,omitempty" title:"Metadata Headers" description:"Extra metadata key/value pairs sent with the call"`
	Request     RequestMsg `json:"request" required:"true" title:"Request message" description:""`
}

type Response struct {
	Context  Context     `json:"context"`
	Response ResponseMsg `json:"response"`
}

type Component struct {
	settings Settings
	//
	servicesAvailable []string
	methodsAvailable  []string
	//
	currentService string
	currentMethod  string
	//
	currentMethodDesc protoreflect.MethodDescriptor
	//
	clientConn *grpc.ClientConn
}

func (h *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "gRPC request",
		Info: "Calls a unary gRPC method on a remote server. Services, methods and message schemas are discovered at configuration time via server reflection, so no proto files are needed — the server must have the reflection service enabled. Connections use TLS with system root certificates by default; enable 'Insecure mode' in settings to talk to plaintext (non-TLS) servers. Each request can attach call metadata: a Bearer Token field (sent as 'authorization: Bearer <token>') and arbitrary key/value headers. An optional Keep Alive setting sends HTTP/2 pings to hold long-lived connections open. Only unary methods are supported — client, server and bidirectional streaming methods are not.",
		Tags:        []string{"grpc", "client"},
	}
}

// OnSettings stores the component settings.
func (h *Component) OnSettings(ctx context.Context, msg any) error {

	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	err := h.connectAndDiscover(ctx, &in)
	h.settings = in
	return err
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

	data, err := h.invoke(ctx, in)
	if err != nil {
		// invoke classifies the failure by gRPC status code (transient codes are
		// marked module.Retryable, client-fault codes module.Permanent), so both
		// the Fail path and the error-port payload carry retryability.
		if !h.settings.EnableErrorPort {
			return module.Fail(err)
		}
		return handler(ctx, ErrorPort, module.NewError(in.Context, err))
	}
	return handler(ctx, ResponsePort, Response{
		Response: ResponseMsg{
			MessageDescriptor{
				Output: data,
			},
		},
		Context: in.Context,
	})
}

func (h *Component) invoke(ctx context.Context, req Request) ([]byte, error) {
	if h.currentMethodDesc == nil {
		return nil, fmt.Errorf("no method descriptor configured")
	}
	//
	input := h.currentMethodDesc.Input()
	inputMsg := dynamicpb.NewMessage(input)

	data, err := json.Marshal(req.Request)
	if err != nil {
		return nil, err
	}

	if err := jsonpb.Unmarshal(bytes.NewReader(data), inputMsg); err != nil {
		return nil, fmt.Errorf("proto unmarshal: %w", err)
	}

	if headers := requestMetadataHeaders(req); len(headers) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, grpcurl.MetadataFromHeaders(headers))
	}
	//
	resp, err := grpcdynamic.NewStub(h.clientConn).InvokeRpc(ctx, h.currentMethodDesc, inputMsg)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.Unavailable, codes.ResourceExhausted:
				// The server refused the request before doing the work —
				// a backoff retry can clear it without double-executing.
				// DeadlineExceeded and Aborted are deliberately NOT marked:
				// they can fire after the method started executing, and this
				// client calls arbitrary reflected methods whose idempotency
				// is unknowable — a retry could apply a side effect twice.
				return nil, module.Retryable(err)
			case codes.InvalidArgument, codes.NotFound, codes.PermissionDenied,
				codes.Unauthenticated, codes.Unimplemented, codes.FailedPrecondition:
				// Client-fault codes — retrying the same request cannot succeed.
				return nil, module.Permanent(err)
			}
			return nil, err
		}
		// No gRPC status at all: the call never reached a server reply
		// (dial/transport failure) — transient by nature.
		return nil, module.Retryable(err)
	}

	respData, err := protojson.Marshal(resp)
	if err != nil {
		return nil, err
	}

	return respData, nil
}

func (h *Component) Ports() []module.Port {

	h.settings.Service = ServiceName{
		Enum{
			Value:   h.currentService,
			Options: h.servicesAvailable,
		},
	}
	h.settings.Method = MethodName{
		Enum{
			Value:   h.currentMethod,
			Options: h.methodsAvailable,
		},
	}
	//
	response := ResponseMsg{}
	request := RequestMsg{}

	if h.currentMethodDesc != nil {
		request.Descriptor = h.currentMethodDesc.Input()
		response.Descriptor = h.currentMethodDesc.Output()
	}

	ports := []module.Port{
		{
			Name:     RequestPort,
			Label:    "Request",
			Position: module.Left,
			Configuration: Request{
				Request: request,
			},
		},
		{
			Name:     ResponsePort,
			Position: module.Right,
			Label:    "Response",
			Source:   true,
			Configuration: Response{
				Response: response,
			},
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
		Configuration: module.ErrorMessage{},
	})
}

// buildDialOptions derives the dial options every connection of this component
// shares: transport credentials (TLS with system roots by default, plaintext
// when Insecure is set) and optional client-side keepalive pings.
func buildDialOptions(settings *Settings) []grpc.DialOption {
	opts := make([]grpc.DialOption, 0, 2)
	if settings.Insecure {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	}
	if settings.KeepAlive {
		opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}))
	}
	return opts
}

// requestMetadataHeaders flattens the request's auth/metadata fields into the
// "key: value" strings grpcurl.MetadataFromHeaders expects. BearerToken is a
// convenience that becomes an "authorization: Bearer <token>" pair; an
// explicit authorization header in Headers takes precedence over it.
func requestMetadataHeaders(req Request) []string {
	headers := make([]string, 0, len(req.Headers)+1)
	explicitAuth := false
	for _, hdr := range req.Headers {
		if hdr.Key == "" {
			continue
		}
		if strings.EqualFold(hdr.Key, "authorization") {
			explicitAuth = true
		}
		headers = append(headers, fmt.Sprintf("%s: %s", hdr.Key, hdr.Value))
	}
	if req.BearerToken != "" && !explicitAuth {
		headers = append(headers, fmt.Sprintf("authorization: Bearer %s", req.BearerToken))
	}
	return headers
}

func (h *Component) connectAndDiscover(ctx context.Context, settings *Settings) error {
	var addr = settings.Address

	if addr == "" {
		return fmt.Errorf("server address is empty")
	}

	conn, err := grpc.NewClient(addr, buildDialOptions(settings)...)
	if err != nil {
		return err
	}
	//
	h.clientConn = conn

	// Reflection/discovery runs at settings time, before any Request message
	// exists — auth metadata lives on the Request port, so there is nothing
	// to attach here. Servers that gate reflection behind auth will need it
	// added at the settings level if that ever comes up.
	refClient := grpcreflect.NewClientAuto(ctx, conn)

	defer refClient.Reset()

	allServices, err := refClient.ListServices()
	if err != nil {
		return err
	}

	var (
		serviceNames []string
	)
	for _, svc := range allServices {
		if svc == "grpc.reflection.v1alpha.ServerReflection" || svc == "grpc.reflection.v1.ServerReflection" {
			continue
		}
		serviceNames = append(serviceNames, string(svc))
	}

	h.currentService = settings.Service.Value

	if len(serviceNames) == 0 {
		return fmt.Errorf("no services discovered")
	}

	sort.Strings(serviceNames)
	h.servicesAvailable = serviceNames

	if h.currentService == "" {
		return fmt.Errorf("select a service")
	}

	//
	h.currentMethod = settings.Method.Value
	h.currentMethodDesc = nil

	for _, service := range serviceNames {

		if service != h.currentService {
			continue
		}

		serviceSymbol, err := refClient.FileContainingSymbol(protoreflect.FullName(service))
		if err != nil {
			return err
		}

		serviceDesc := serviceSymbol.Services()
		if serviceDesc == nil {
			continue
		}

		svcDesc := serviceDesc.ByName(protoreflect.FullName(service).Name())
		if svcDesc == nil {
			continue
		}

		//
		methodsDescs := svcDesc.Methods()
		if methodsDescs == nil {
			continue
		}

		var allMethods []string

		for i := 0; i < methodsDescs.Len(); i++ {

			methodDescriptor := methodsDescs.Get(i)
			if methodDescriptor == nil {
				continue
			}
			methodName := string(methodDescriptor.Name())
			allMethods = append(allMethods, methodName)

			if methodName == h.currentMethod {
				h.currentMethodDesc = methodDescriptor
			}
		}

		h.methodsAvailable = allMethods

		//
		if h.currentMethod == "" {
			return fmt.Errorf("select method")
		}

		if len(allMethods) == 0 {
			return nil
		}

		if h.currentMethodDesc == nil {
			return fmt.Errorf("selected method description not found")
		}
		return nil
	}

	return fmt.Errorf("selected service %s not found", h.currentService)
}

func (h *Component) Instance() module.Component {
	return &Component{}
}

var _ jsonschema.Exposer = (*ServiceName)(nil)
var _ jsonschema.Exposer = (*MethodName)(nil)

var _ json.Marshaler = (*ServiceName)(nil)
var _ json.Unmarshaler = (*ServiceName)(nil)

var _ jsonschema.Exposer = (*RequestMsg)(nil)
var _ jsonschema.Exposer = (*ResponseMsg)(nil)

var _ json.Marshaler = (*ResponseMsg)(nil)
var _ json.Unmarshaler = (*ResponseMsg)(nil)
var _ json.Marshaler = (*RequestMsg)(nil)
var _ json.Unmarshaler = (*RequestMsg)(nil)

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
)

func init() {
	registry.Register(&Component{})
}
