package config

type TikvClientOptions struct {
	UseTxnApi bool `mapstructure:"useTxnApi"`

	// tikv client conf
	PDAddrs string `mapstructure:"pdAddrs"`
	//CompletionQueueSize  int    `mapstructure:"completionQueueSize"`
	//GrpcKeepaliveTime    int    `mapstructure:"grpcKeepaliveTime"`
	//GrpcKeepaliveTimeout int    `mapstructure:"grpcKeepaliveTimeout"`
	//AllowBatch           bool   `mapstructure:"allowBatch"`
	//MaxBatchWaitTime     int    `mapstructure:"maxBatchWaitTime"`
	//MaxBatchSize         int    `mapstructure:"maxBatchSize"`
	//MaxInflightRequests  int    `mapstructure:"maxInflightRequests"`

	// txn conf
	UseAsyncCommit    bool `mapstructure:"useAsyncCommit"`
	TryOnePcCommit    bool `mapstructure:"tryOnePcCommit"`
	UsePessimisticTxn bool `mapstructure:"useAsyncCommit"`
	//TxnRetryCn int `mapstructure:"txRetryCn"`
	//TxnRegionBackoffDelayMs      int  `mapstructure:"txnRegionBackoffDelayMs"`
	//TxnRegionBackoffDelayAttemps int  `mapstructure:"txnRegionBackoffDelayAttemps"`
	//TxnLockBackoffDelayMs        int  `mapstructure:"txnLockBackoffDelayMs"`
	//TxnLockBackoffDelayAttemps   int  `mapstructure:"txnLockBackoffDelayAttemps"`
}
type LeaderJobOptions struct {
	LeaderCheckInterval int `mapstructure:"leaderCheckInterval"`
	LeaderLeaseDuration int `mapstructure:"leaderLeaseDuration"`
}

type GCJobOptions struct {
	GCEnabled           bool `mapstructure:"gcEnabled"`
	GCInterval          int  `mapstructure:"gcInterval"`
	GCConcurrency       int  `mapstructure:"gcConcurrency"`
	GCSafePointLifeTime int  `mapstructure:"gcSafepointLifeTime"`
}

type StoragerOptions struct {
	Databases        int `mapstructure:"databases"`
	TTLCheckInterval int `mapstructure:"ttlCheckInterval"`

	TiKVClient TikvClientOptions `mapstructure:"tikvClientOpts"`
	GCJob      GCJobOptions      `mapstructure:"gcJobOpts"`
	LeaderJob  LeaderJobOptions  `mapstructure:"leaderJobOpts"`
}

func DefaultTikvClientOptions() *TikvClientOptions {
	return &TikvClientOptions{
		UseTxnApi:         true,
		PDAddrs:           "127.0.0.1:2379",
		UseAsyncCommit:    false,
		TryOnePcCommit:    false,
		UsePessimisticTxn: false,
	}
}

func DefaultStoragerOptions() *StoragerOptions {
	return &StoragerOptions{
		Databases:        16,
		TTLCheckInterval: 10,
		TiKVClient:       *DefaultTikvClientOptions(),
		GCJob:            *DefaultGCJobOptions(),
		LeaderJob:        *DefaultLeaderJobOptions(),
	}
}

func DefaultGCJobOptions() *GCJobOptions {
	return &GCJobOptions{
		GCEnabled:           false,
		GCInterval:          600, //10m like tidb gc
		GCConcurrency:       8,
		GCSafePointLifeTime: 600,
	}
}

func DefaultLeaderJobOptions() *LeaderJobOptions {
	return &LeaderJobOptions{
		LeaderCheckInterval: 30,
		LeaderLeaseDuration: 60, // lease > check interval
	}
}
