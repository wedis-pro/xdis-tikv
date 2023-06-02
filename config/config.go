package config

type TikvClientOptions struct {
	UseTxnApi                    bool   `mapstructure:"useTxnApi"`
	UseAsyncCommit               bool   `mapstructure:"useAsyncCommit"`
	TryOnePcCommit               bool   `mapstructure:"tryOnePcCommit"`
	UsePessimisticTxn            bool   `mapstructure:"useAsyncCommit"`
	LocalPoolNumber              int    `mapstructure:"localPoolNumber"`
	TxnRetryCn                   string `mapstructure:"txRetryCn"`
	TxnRegionBackoffDelayMs      int    `mapstructure:"txnRegionBackoffDelayMs"`
	TxnRegionBackoffDelayAttemps int    `mapstructure:"txnRegionBackoffDelayAttemps"`
	TxnLockBackoffDelayMs        int    `mapstructure:"txnLockBackoffDelayMs"`
	TxnLockBackoffDelayAttemps   int    `mapstructure:"txnLockBackoffDelayAttemps"`

	CompletionQueueSize  int  `mapstructure:"completionQueueSize"`
	GrpcKeepaliveTime    int  `mapstructure:"grpcKeepaliveTime"`
	GrpcKeepaliveTimeout int  `mapstructure:"grpcKeepaliveTimeout"`
	AllowBatch           bool `mapstructure:"allowBatch"`
	MaxBatchWaitTime     int  `mapstructure:"maxBatchWaitTime"`
	MaxBatchSize         int  `mapstructure:"maxBatchSize"`
	MaxInflightRequests  int  `mapstructure:"maxInflightRequests"`

	Databases        int `mapstructure:"databases"`
	TTLCheckInterval int `mapstructure:"ttlCheckInterval"`
}

func DefaultTikvClientOptions() *TikvClientOptions {
	return &TikvClientOptions{}
}
