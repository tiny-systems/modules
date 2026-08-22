package etc

type ClientConfig struct {
	Credentials string   `json:"credentials" required:"true" format:"textarea" title:"Credentials" description:"Google client credentials.json or service account key file content"`
	Scopes      []string `json:"scopes,omitempty" title:"Scopes"`
	Subject     string   `json:"subject,omitempty" title:"Subject" description:"Email to impersonate via domain-wide delegation (service accounts only)"`
}
