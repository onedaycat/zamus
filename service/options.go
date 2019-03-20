package service

type HandlerOption func(opt *handlerOptions)

type handlerOptions struct {
	mergeHandler MergeBatchHandler
	prehandlers  []Handler
}

func WithMergeBatchHandler(handler MergeBatchHandler) HandlerOption {
	return func(opt *handlerOptions) {
		opt.mergeHandler = handler
	}
}

func WithPrehandler(handlers ...Handler) HandlerOption {
	return func(opt *handlerOptions) {
		opt.prehandlers = handlers
	}
}
