package certgenerate

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"

	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "cert_generate"
	RequestPort   = "request"
	ResultPort    = "result"
)

type Context any

type Request struct {
	Context    Context  `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	CommonName string   `json:"commonName" required:"true" title:"Common Name" description:"Certificate CN (e.g. webhook)" colSpan:"col-span-6"`
	ValidDays  int      `json:"validDays" required:"true" title:"Valid Days" description:"Certificate validity in days" colSpan:"col-span-6"`
	DNSNames   []string `json:"dnsNames,omitempty" title:"DNS Names" description:"Subject Alternative Names (SANs)"`
}

type Result struct {
	Context  Context `json:"context,omitempty" title:"Context"`
	CertPEM  string  `json:"certPEM" title:"Certificate" description:"PEM-encoded certificate"`
	KeyPEM   string  `json:"keyPEM" title:"Private Key" description:"PEM-encoded private key"`
	CABundle string  `json:"caBundle" title:"CA Bundle" description:"Base64-encoded certificate for webhook caBundle"`
}

type Component struct{}

func (c *Component) Instance() module.Component {
	return &Component{}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Certificate Generator",
		Info:        "Generate a self-signed TLS certificate with optional SANs. Use for K8s admission webhooks, internal HTTPS servers, or any TLS endpoint.",
		Tags:        []string{"TLS", "Certificate", "Crypto", "Webhook"},
	}
}

func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	switch port {
	case RequestPort:
		in, ok := msg.(Request)
		if !ok {
			return module.Fail(fmt.Errorf("invalid request"))
		}
		return c.handleRequest(ctx, handler, in)
	}
	return module.Fail(fmt.Errorf("unknown port: %s", port))
}

func (c *Component) handleRequest(ctx context.Context, handler module.Handler, req Request) module.Result {
	validDays := req.ValidDays
	if validDays <= 0 {
		validDays = 3650
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return module.Fail(fmt.Errorf("generate key: %w", err))
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return module.Fail(fmt.Errorf("generate serial: %w", err))
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      pkix.Name{CommonName: req.CommonName},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Duration(validDays) * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     req.DNSNames,
		IsCA:         true,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return module.Fail(fmt.Errorf("create certificate: %w", err))
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return module.Fail(fmt.Errorf("marshal key: %w", err))
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	caBundle := base64.StdEncoding.EncodeToString(certPEM)

	return handler(ctx, ResultPort, Result{
		Context:  req.Context,
		CertPEM:  string(certPEM),
		KeyPEM:   string(keyPEM),
		CABundle: caBundle,
	})
}

func (c *Component) Ports() []module.Port {
	return []module.Port{
		{
			Name:  RequestPort,
			Label: "Request",
			Configuration: Request{
				CommonName: "webhook",
				ValidDays:  3650,
				DNSNames:   []string{"my-service.my-namespace.svc", "my-service.my-namespace.svc.cluster.local"},
			},
			Position: module.Left,
		},
		{
			Name:   ResultPort,
			Label:  "Result",
			Source: true,
			Configuration: Result{
				CertPEM:  "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
				KeyPEM:   "-----BEGIN EC PRIVATE KEY-----\n...\n-----END EC PRIVATE KEY-----",
				CABundle: "LS0tLS1CRUdJTi...",
			},
			Position: module.Right,
		},
	}
}

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register(&Component{})
}
