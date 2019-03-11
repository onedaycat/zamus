package warmer

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	jsoniter "github.com/json-iterator/go"
	"github.com/onedaycat/zamus/invoke"
)

type Warmer struct {
	delay      time.Duration
	ld         invoke.Invoker
	wg         sync.WaitGroup
	invokeType string
}

func New(sess *session.Session) *Warmer {
	return &Warmer{
		delay:      75 * time.Millisecond,
		ld:         lambda.New(sess),
		invokeType: "Event",
	}
}

type WarmerRequest struct {
	Warmer        bool   `json:"warmer"`
	Concurency    int    `json:"concurency"`
	CorrelationID string `json:"correlationID"`
}

func (w *Warmer) Run(ctx context.Context, concurency int, correlationID string) {
	if correlationID == "" {
		return
	}

	if concurency == 0 {
		return
	}

	lc, _ := lambdacontext.FromContext(ctx)

	json := jsoniter.ConfigCompatibleWithStandardLibrary
	payload, _ := json.Marshal(WarmerRequest{
		Warmer:        true,
		Concurency:    concurency,
		CorrelationID: lc.AwsRequestID,
	})

	if concurency == 1 {
		w.invoke(ctx, payload)
		return
	}

	w.wg.Add(concurency)

	for i := 0; i < concurency; i++ {
		go w.conInvoke(ctx, payload)
	}

	w.wg.Wait()
}

func (w *Warmer) conInvoke(ctx context.Context, payload []byte) {
	defer w.wg.Done()
	w.ld.Invoke(&lambda.InvokeInput{
		FunctionName:   &lambdacontext.FunctionName,
		Qualifier:      &lambdacontext.FunctionVersion,
		InvocationType: &w.invokeType,
		Payload:        payload,
	})
}

func (w *Warmer) invoke(ctx context.Context, payload []byte) {
	w.ld.Invoke(&lambda.InvokeInput{
		FunctionName:   &lambdacontext.FunctionName,
		Qualifier:      &lambdacontext.FunctionVersion,
		InvocationType: &w.invokeType,
		Payload:        payload,
	})
}
