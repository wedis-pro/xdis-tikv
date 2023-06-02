package config

type TikvClientOptions struct {
	UseTxnApi bool `mapstructure:"useTxnApi"`

	// tikv client conf
	PDAddrs              string `mapstructure:"pdAddrs"`
	CompletionQueueSize  int    `mapstructure:"completionQueueSize"`
	GrpcKeepaliveTime    int    `mapstructure:"grpcKeepaliveTime"`
	GrpcKeepaliveTimeout int    `mapstructure:"grpcKeepaliveTimeout"`
	AllowBatch           bool   `mapstructure:"allowBatch"`
	MaxBatchWaitTime     int    `mapstructure:"maxBatchWaitTime"`
	MaxBatchSize         int    `mapstructure:"maxBatchSize"`
	MaxInflightRequests  int    `mapstructure:"maxInflightRequests"`

	// txn client conf
	TxnRetryCn                   string `mapstructure:"txRetryCn"`
	TxnRegionBackoffDelayMs      int    `mapstructure:"txnRegionBackoffDelayMs"`
	TxnRegionBackoffDelayAttemps int    `mapstructure:"txnRegionBackoffDelayAttemps"`
	TxnLockBackoffDelayMs        int    `mapstructure:"txnLockBackoffDelayMs"`
	TxnLockBackoffDelayAttemps   int    `mapstructure:"txnLockBackoffDelayAttemps"`
	UseAsyncCommit               bool   `mapstructure:"useAsyncCommit"`
	TryOnePcCommit               bool   `mapstructure:"tryOnePcCommit"`
	UsePessimisticTxn            bool   `mapstructure:"useAsyncCommit"`
}

type StoragerOptions struct {
	Databases        int `mapstructure:"databases"`
	TTLCheckInterval int `mapstructure:"ttlCheckInterval"`

	TiKVClient TikvClientOptions `mapstructure:"tikvClientOpts"`
}

func DefaultTikvClientOptions() *TikvClientOptions {
	return &TikvClientOptions{}
}
