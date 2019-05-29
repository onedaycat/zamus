package source

import (
    "context"
    "encoding/json"
    "testing"

    "github.com/aws/aws-lambda-go/events"
    "github.com/onedaycat/zamus/zamus"
    "github.com/stretchr/testify/require"
)

func TestJSONHandler(t *testing.T) {
    sh := func(ctx context.Context, src json.RawMessage) (interface{}, error) {
        return map[string]interface{}{
            "id": "1",
        }, nil
    }

    h := zamus.New(NewJSONHandler(sh))

    t.Run("Success", func(t *testing.T) {
        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`{"id":"1"}`))

        require.NoError(t, err)
        require.Equal(t, map[string]interface{}{
            "id": "1",
        }, result)
    })
}

func TestS3Handler(t *testing.T) {
    sh := func(ctx context.Context, src *events.S3Event) (interface{}, error) {
        return map[string]interface{}{
            "id": "1",
        }, nil
    }

    h := zamus.New(NewS3EventHandler(sh))

    t.Run("Success", func(t *testing.T) {
        ctx := context.Background()
        result, err := h.Invoke(ctx, []byte(`{"id":"1"}`))

        require.NoError(t, err)
        require.Equal(t, map[string]interface{}{
            "id": "1",
        }, result)
    })
}
