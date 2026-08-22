package encode

import (
	"encoding/json"
	"fmt"
	"github.com/golang-jwt/jwt/v5"
	"github.com/swaggest/jsonschema-go"
	"math"
	"time"
)

// MapClaims is a claims type that uses the map[string]interface{} for JSON
// decoding. This is the default claims type if you don't supply one
type MapClaims map[string]interface{}

// GetExpirationTime implements the Claims interface.
func (m MapClaims) GetExpirationTime() (*jwt.NumericDate, error) {
	return m.parseNumericDate("exp")
}

// GetNotBefore implements the Claims interface.
func (m MapClaims) GetNotBefore() (*jwt.NumericDate, error) {
	return m.parseNumericDate("nbf")
}

// GetIssuedAt implements the Claims interface.
func (m MapClaims) GetIssuedAt() (*jwt.NumericDate, error) {
	return m.parseNumericDate("iat")
}

// GetAudience implements the Claims interface.
func (m MapClaims) GetAudience() (jwt.ClaimStrings, error) {
	return m.parseClaimsString("aud")
}

// GetIssuer implements the Claims interface.
func (m MapClaims) GetIssuer() (string, error) {
	return m.parseString("iss")
}

// GetSubject implements the Claims interface.
func (m MapClaims) GetSubject() (string, error) {
	return m.parseString("sub")
}

// parseNumericDate tries to parse a key in the map claims type as a number
// date. This will succeed, if the underlying type is either a [float64] or a
// [json.Number]. Otherwise, nil will be returned.
func (m MapClaims) parseNumericDate(key string) (*jwt.NumericDate, error) {
	v, ok := m[key]
	if !ok {
		return nil, nil
	}

	switch exp := v.(type) {
	case float64:
		if exp == 0 {
			return nil, nil
		}

		return newNumericDateFromSeconds(exp), nil
	case json.Number:
		v, _ := exp.Float64()

		return newNumericDateFromSeconds(v), nil
	}

	return nil, newError(fmt.Sprintf("%s is invalid", key), jwt.ErrInvalidType)
}

// parseClaimsString tries to parse a key in the map claims type as a
// [ClaimsStrings] type, which can either be a string or an array of string.
func (m MapClaims) parseClaimsString(key string) (jwt.ClaimStrings, error) {
	var cs []string
	switch v := m[key].(type) {
	case string:
		cs = append(cs, v)
	case []string:
		cs = v
	case []interface{}:
		for _, a := range v {
			vs, ok := a.(string)
			if !ok {
				return nil, newError(fmt.Sprintf("%s is invalid", key), jwt.ErrInvalidType)
			}
			cs = append(cs, vs)
		}
	}

	return cs, nil
}

// parseString tries to parse a key in the map claims type as a [string] type.
// If the key does not exist, an empty string is returned. If the key has the
// wrong type, an error is returned.
func (m MapClaims) parseString(key string) (string, error) {
	var (
		ok  bool
		raw interface{}
		iss string
	)
	raw, ok = m[key]
	if !ok {
		return "", nil
	}

	iss, ok = raw.(string)
	if !ok {
		return "", newError(fmt.Sprintf("%s is invalid", key), jwt.ErrInvalidType)
	}

	return iss, nil
}

// newNumericDateFromSeconds creates a new *NumericDate out of a float64 representing a
// UNIX epoch with the float fraction representing non-integer seconds.
func newNumericDateFromSeconds(f float64) *jwt.NumericDate {
	round, frac := math.Modf(f)
	return jwt.NewNumericDate(time.Unix(int64(round), int64(frac*1e9)))
}

func newError(message string, err error, more ...error) error {
	var format string
	var args []any
	if message != "" {
		format = "%w: %s"
		args = []any{err, message}
	} else {
		format = "%w"
		args = []any{err}
	}

	for _, e := range more {
		format += ": %w"
		args = append(args, e)
	}

	err = fmt.Errorf(format, args...)
	return err
}

// JSONSchema redners default json schema
func (m MapClaims) JSONSchema() (jsonschema.Schema, error) {
	name := jsonschema.Schema{}
	name.AddType(jsonschema.Object)
	name.WithProperties(map[string]jsonschema.SchemaOrBool{
		"sub": (&jsonschema.Schema{}).WithTitle("Subject").WithType(jsonschema.String.Type()).ToSchemaOrBool(),
		"iss": (&jsonschema.Schema{}).WithTitle("Issuer").WithType(jsonschema.String.Type()).ToSchemaOrBool(),
		"aud": (&jsonschema.Schema{}).WithTitle("Audience").WithType(jsonschema.Array.Type()).WithItems(*(&jsonschema.Items{}).WithSchemaOrBool((&jsonschema.Schema{}).WithType(jsonschema.String.Type()).ToSchemaOrBool())).ToSchemaOrBool(),
		"exp": (&jsonschema.Schema{}).WithTitle("ExpiresAt").WithType(jsonschema.Integer.Type()).WithDescription("Expiration time").ToSchemaOrBool(),
		"nbf": (&jsonschema.Schema{}).WithTitle("NotBefore").WithType(jsonschema.Integer.Type()).ToSchemaOrBool(),
		"iat": (&jsonschema.Schema{}).WithTitle("IssuedAt").WithType(jsonschema.Integer.Type()).ToSchemaOrBool(),
		"jti": (&jsonschema.Schema{}).WithTitle("ID").WithType(jsonschema.String.Type()).ToSchemaOrBool(),
	})
	return name, nil
}

/*
Issuer string `json:"iss,omitempty"`

	// the `sub` (Subject) claim. See https://datatracker.ietf.org/doc/html/rfc7519#section-4.1.2
	Subject string `json:"sub,omitempty"`

	// the `aud` (Audience) claim. See https://datatracker.ietf.org/doc/html/rfc7519#section-4.1.3
	Audience ClaimStrings `json:"aud,omitempty"`

	// the `exp` (Expiration Time) claim. See https://datatracker.ietf.org/doc/html/rfc7519#section-4.1.4
	ExpiresAt *NumericDate `json:"exp,omitempty"`

	// the `nbf` (Not Before) claim. See https://datatracker.ietf.org/doc/html/rfc7519#section-4.1.5
	NotBefore *NumericDate `json:"nbf,omitempty"`

	// the `iat` (Issued At) claim. See https://datatracker.ietf.org/doc/html/rfc7519#section-4.1.6
	IssuedAt *NumericDate `json:"iat,omitempty"`

	// the `jti` (JWT ID) claim. See https://datatracker.ietf.org/doc/html/rfc7519#section-4.1.7
	ID string `json:"jti,omitempty"
*/
