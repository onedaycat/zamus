package invoke

import (
	"testing"

	"github.com/onedaycat/zamus/common"
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
		   "email":"test@test.com",
		   "pem": {
			   "shop1": "read,open"
		   }
		},
		"sourceIp":[
		   "x.x.x.x"
		],
		"defaultAuthStrategy":"ALLOW",
		"groups": ["admin"]
	 }
	`)

	id := &Identity{}

	err := common.UnmarshalJSON(payload, id)
	if err != nil {
		require.NoError(t, err)
	}

	require.Equal(t, &Identity{
		Sub:      "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
		Username: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
		Claims: Claims{
			Email:     "test@test.com",
			CreatedAt: 1547875744,
			ExpiredAt: 1547879344,
			Permissions: Permissions{
				"shop1": "read,open",
			},
		},
		SourceIP: []string{"x.x.x.x"},
		Groups:   []string{"admin"},
	}, id)

	require.Equal(t, "test@test.com", id.GetEmail())
	require.Equal(t, "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx", id.GetID())
	require.Equal(t, "x.x.x.x", id.GetIP())
	require.True(t, id.HasGroup("admin"))
	require.False(t, id.HasGroup("user"))
	require.True(t, id.Claims.Permissions.Has("shop1", "read"))
	require.True(t, id.Claims.Permissions.Has("shop1", "open"))
	require.False(t, id.Claims.Permissions.Has("shop1", "write"))
	require.False(t, id.Claims.Permissions.Has("shop2", "read"))
}
