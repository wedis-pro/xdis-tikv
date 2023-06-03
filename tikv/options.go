package tikv

type options struct {
	tryOnePcCommit bool
	asyncCommit    bool
	pessimisticTxn bool
}

// TxnOpt tikv begin txn opt
type TxnOpt func(*options)

func WithTryOnePcCommit(onePC bool) TxnOpt {
	return func(opt *options) {
		opt.tryOnePcCommit = onePC
	}
}

func WithAsyncCommit(async bool) TxnOpt {
	return func(opt *options) {
		opt.asyncCommit = async
	}
}

func WithPessimisticTxn(pessimictic bool) TxnOpt {
	return func(opt *options) {
		opt.pessimisticTxn = pessimictic
	}
}
