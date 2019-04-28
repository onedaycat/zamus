package ses

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSubject(t *testing.T) {
	data := ` <!--    SellConnect: {{storeName}} has invied you -->  
<h1>Hello</h1>
<p>Your have an invite from {{ storeName }}.</p>
<a href="{{ inviteURL }}?code={{ inviteCode }}">Click this link to accept invite</a>`

	expSubj := "SellConnect: {{storeName}} has invied you"
	expTmpl := `<h1>Hello</h1>
<p>Your have an invite from {{ storeName }}.</p>
<a href="{{ inviteURL }}?code={{ inviteCode }}">Click this link to accept invite</a>`

	subj, tmpl := getSplitSubjectAndTemplte(data)

	require.Equal(t, expSubj, subj)
	require.Equal(t, expTmpl, tmpl)
}
