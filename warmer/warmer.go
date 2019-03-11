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
	Warmer     bool `json:"warmer"`
	Concurency int  `json:"concurency"`
}

func (w *Warmer) Run(ctx context.Context, concurency int) {
	if concurency == 0 {
		return
	}

	json := jsoniter.ConfigCompatibleWithStandardLibrary
	payload, _ := json.Marshal(WarmerRequest{
		Warmer:     true,
		Concurency: 0,
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
