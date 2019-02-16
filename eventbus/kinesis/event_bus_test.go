package kinesis

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/onedaycat/gocqrs"
	"github.com/stretchr/testify/require"
)

func TestPublish(t *testing.T) {
	sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewEnvCredentials(),
		Region:      aws.String("ap-southeast-1"),
	})
	require.NoError(t, err)
	k := NewKinesisEventBus(sess, "eventsourcing")

	events := []*gocqrs.EventMessage{
		{EventID: "1", AggregateID: "a1"},
		{EventID: "2", AggregateID: "a2"},
		{EventID: "3", AggregateID: "a3"},
		{EventID: "4", AggregateID: "a1"},
	}

	err = k.Publish(events)
	require.NoError(t, err)
}
