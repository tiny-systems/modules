package etc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// credentialFile is used to detect the credential type from JSON.
type credentialFile struct {
	Type string `json:"type"`
}

// NewGoogleHTTPClient returns an authenticated *http.Client based on the credential type.
// Service account JSON uses JWT with optional subject impersonation.
// OAuth2 JSON uses the provided token.
func NewGoogleHTTPClient(ctx context.Context, config ClientConfig, token *Token) (*http.Client, error) {
	var cf credentialFile
	if err := json.Unmarshal([]byte(config.Credentials), &cf); err != nil {
		return nil, fmt.Errorf("unable to parse credentials JSON: %v", err)
	}
	if cf.Type == "service_account" {
		return newServiceAccountClient(ctx, config)
	}
	return newOAuth2Client(ctx, config, token)
}

func newServiceAccountClient(ctx context.Context, config ClientConfig) (*http.Client, error) {
	jwtConfig, err := google.JWTConfigFromJSON([]byte(config.Credentials), config.Scopes...)
	if err != nil {
		return nil, fmt.Errorf("unable to parse service account key: %v", err)
	}
	if config.Subject != "" {
		jwtConfig.Subject = config.Subject
	}
	return jwtConfig.Client(ctx), nil
}

func newOAuth2Client(ctx context.Context, config ClientConfig, token *Token) (*http.Client, error) {
	if token == nil {
		return nil, fmt.Errorf("OAuth2 credentials require a token")
	}
	oauthConfig, err := google.ConfigFromJSON([]byte(config.Credentials), config.Scopes...)
	if err != nil {
		return nil, fmt.Errorf("unable to parse client secret file to config: %v", err)
	}
	return oauthConfig.Client(ctx, &oauth2.Token{
		AccessToken:  token.AccessToken,
		RefreshToken: token.RefreshToken,
		Expiry:       token.Expiry,
		TokenType:    token.TokenType,
	}), nil
}
