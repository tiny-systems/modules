package etc

import "time"

type Token struct {
	AccessToken  string    `json:"access_token" required:"true" minLength:"1" title:"AccessToken" description:"Token that authorizes and authenticates"`
	TokenType    string    `json:"token_type" required:"true" title:"TokenType" enum:"Bearer" description:"The Type method returns either this or \"Bearer\", the default"`
	RefreshToken string    `json:"refresh_token,omitempty" title:"RefreshToken" description:"Token that's used by the application (as opposed to the user) to refresh the access token if it expires."`
	Expiry       time.Time `json:"expiry,omitempty" title:"Expiry" description:"Expiry is the optional expiration time of the access token. If zero, TokenSource implementations will reuse the same token forever and RefreshToken or equivalent mechanisms for that TokenSource will not be used. 2012-10-01T09:45:00.000+02:00"`
}
