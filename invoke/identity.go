package invoke

type Claims struct {
	Email     string `json:"email"`
	ExpiredAt int    `json:"exp"`
	CreatedAt int    `json:"iat"`
}

type Identity struct {
	Sub                   string   `json:"sub"`
	AccountId             string   `json:"accountId"`
	CognitoIdentityPoolId string   `json:"cognitoIdentityPoolId"`
	CognitoIdentityId     string   `json:"cognitoIdentityId"`
	SourceIP              []string `json:"sourceIp"`
	Groups                []string `json:"groups"`
	Username              string   `json:"username"`
	UserArn               string   `json:"userArn"`
	Issuer                string   `json:"issuer"`
	Claims                Claims   `json:"claims"`
	DefaultAuthStrategy   string   `json:"defaultAuthStrategy"`
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
