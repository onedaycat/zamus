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
	concurrent int
	wg         sync.WaitGroup
	invokeType string
}

func New(sess *session.Session, concurrent int) *Warmer {
	return &Warmer{
		delay:      75 * time.Millisecond,
		ld:         lambda.New(sess),
		concurrent: concurrent,
		invokeType: "Event",
	}
}

type WarmerRequest struct {
	Warmer        bool   `json:"warmer"`
	Concurency    int    `json:"concurency"`
	CorrelationId string `json:"correlationID"`
}

func (w *Warmer) Run(ctx context.Context) {
	if w.concurrent == 0 {
		return
	}

	lc, _ := lambdacontext.FromContext(ctx)

	json := jsoniter.ConfigCompatibleWithStandardLibrary
	payload, _ := json.Marshal(WarmerRequest{
		Warmer:        true,
		Concurency:    w.concurrent,
		CorrelationId: lc.AwsRequestID,
	})

	if w.concurrent == 1 {
		w.invoke(ctx, payload)
		return
	}

	w.wg.Add(w.concurrent)

	for i := 0; i < w.concurrent; i++ {
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
