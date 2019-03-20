package warmer

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/onedaycat/zamus/invoke"
)

type Warmer struct {
	delay      time.Duration
	ld         invoke.Invoker
	wg         sync.WaitGroup
	invokeType string
}

func New(ld *lambda.Lambda) *Warmer {
	return &Warmer{
		delay:      75 * time.Millisecond,
		ld:         invoke.NewInvoke(ld),
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

	req := &invoke.Request{
		Warmer:     true,
		Concurency: 0,
	}

	w.wg.Add(concurency)

	for i := 0; i < concurency; i++ {
		go w.conInvoke(ctx, req)
	}

	w.wg.Wait()
}

func (w *Warmer) conInvoke(ctx context.Context, req *invoke.Request) {
	defer w.wg.Done()
	w.ld.InvokeAsync(ctx, lambdacontext.FunctionName, req)
}
