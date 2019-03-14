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
	Sub       string   `json:"sub,omitempty"`
	AccountId string   `json:"accountId,omitempty"`
	SourceIP  []string `json:"sourceIp,omitempty"`
	Groups    []string `json:"groups,omitempty"`
	Username  string   `json:"username,omitempty"`
	Claims    Claims   `json:"claims,omitempty"`
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
