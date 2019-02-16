package invoke

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIdentity(t *testing.T) {
	payload := []byte(`
	{
		"sub":"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
		"issuer":"https://issuer.com",
		"username":"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
		"claims":{
		   "sub":"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
		   "aud":"audxxxxx",
		   "email_verified":true,
		   "event_id":"eventid",
		   "token_use":"id",
		   "auth_time":1547875744,
		   "iss":"https://issuer.com",
		   "cognito:username":"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
		   "cognito:groups": [
    			"admin"
  			],
		   "exp":1547879344,
		   "iat":1547875744,
		   "email":"test@test.com"
		},
		"sourceIp":[
		   "x.x.x.x"
		],
		"defaultAuthStrategy":"ALLOW",
		"groups": ["admin"]
	 }
	`)

	id := &Identity{}

	err := json.Unmarshal(payload, id)
	if err != nil {
		require.NoError(t, err)
	}

	require.Equal(t, &Identity{
		Sub:      "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
		Issuer:   "https://issuer.com",
		Username: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
		Claims: Claims{
			Email:     "test@test.com",
			CreatedAt: 1547875744,
			ExpiredAt: 1547879344,
		},
		SourceIP:            []string{"x.x.x.x"},
		DefaultAuthStrategy: "ALLOW",
		Groups:              []string{"admin"},
	}, id)

	require.Equal(t, "test@test.com", id.GetEmail())
	require.Equal(t, "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx", id.GetID())
	require.Equal(t, "x.x.x.x", id.GetIP())
	require.True(t, id.HasGroup("admin"))
	require.False(t, id.HasGroup("user"))
}
