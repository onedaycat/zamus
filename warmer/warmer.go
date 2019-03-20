package warmer

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/onedaycat/zamus/common"
)

type Invoker interface {
	InvokeAsyncWithContext(ctx context.Context, input *lambda.InvokeAsyncInput, opts ...request.Option) (*lambda.InvokeAsyncOutput, error)
}

type Warmer struct {
	delay      time.Duration
	ld         Invoker
	wg         sync.WaitGroup
	invokeType string
}

func New(ld *lambda.Lambda) *Warmer {
	return &Warmer{
		delay:      75 * time.Millisecond,
		ld:         ld,
		invokeType: "Event",
	}
}

type WarmerRequest struct {
	Warmer     bool `json:"warmer"`
	Concurency int  `json:"concurency"`
}

func (w *Warmer) Run(ctx context.Context, concurency int) {
	if concurency < 2 {
		time.Sleep(w.delay)
		return
	}

	payload, _ := common.MarshalJSON(WarmerRequest{
		Warmer:     true,
		Concurency: 0,
	})

	w.wg.Add(concurency)

	for i := 0; i < concurency; i++ {
		go w.conInvoke(ctx, payload)
	}

	w.wg.Wait()
}

func (w *Warmer) conInvoke(ctx context.Context, payload []byte) {
	defer w.wg.Done()
	w.ld.InvokeAsyncWithContext(ctx, &lambda.InvokeAsyncInput{
		FunctionName: &lambdacontext.FunctionName,
		InvokeArgs:   bytes.NewReader(payload),
	})
}
