package invoke

import "strings"

type Claims struct {
	Email       string      `json:"email,omitempty"`
	ExpiredAt   int         `json:"exp,omitempty"`
	CreatedAt   int         `json:"iat,omitempty"`
	Permissions Permissions `json:"pem,omitempty"`
}

type Permissions map[string]string

func (p Permissions) Has(workspace, permission string) bool {
	ws, ok := p[workspace]
	if !ok {
		return false
	}

	return strings.Contains(ws, permission)
}

type Identity struct {
	Sub                   string   `json:"sub,omitempty"`
	AccountId             string   `json:"accountId,omitempty"`
	CognitoIdentityPoolId string   `json:"cognitoIdentityPoolId,omitempty"`
	CognitoIdentityId     string   `json:"cognitoIdentityId,omitempty"`
	SourceIP              []string `json:"sourceIp,omitempty"`
	Groups                []string `json:"groups,omitempty"`
	Username              string   `json:"username,omitempty"`
	UserArn               string   `json:"userArn,omitempty"`
	Issuer                string   `json:"issuer,omitempty"`
	Claims                Claims   `json:"claims,omitempty"`
	DefaultAuthStrategy   string   `json:"defaultAuthStrategy,omitempty"`
}

func (id *Identity) GetID() string {
	return id.Username
}

func (id *Identity) GetEmail() string {
	return id.Claims.Email
}

func (id *Identity) GetIP() string {
	return id.SourceIP[0]
}

func (id *Identity) HasGroup(group string) bool {
	for _, g := range id.Groups {
		if g == group {
			return true
		}
	}

	return false
}
