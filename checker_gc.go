package xdistikv

import (
	"context"
	"runtime"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/tikv/client-go/v2/oracle"
	tikvClient "github.com/tikv/client-go/v2/tikv"
	"github.com/weedge/xdis-tikv/v1/config"
	"github.com/weedge/xdis-tikv/v1/tikv"
)

type GCChecker struct {
	opts     *config.GCJobOptions
	kvClient *tikv.Client

	leaderChecker *LeaderChecker
}

func NewGCChecker(opts *config.GCJobOptions, client *tikv.Client, leader *LeaderChecker) *GCChecker {
	initOpts(opts)
	return &GCChecker{
		opts:          opts,
		kvClient:      client,
		leaderChecker: leader,
	}
}

func initOpts(opts *config.GCJobOptions) {
	if opts.GCConcurrency <= 0 {
		opts.GCConcurrency = runtime.NumCPU()
	}
	if opts.GCInterval <= 0 {
		opts.GCInterval = 600
	}
	if opts.GCSafePointLifeTime <= 0 {
		opts.GCSafePointLifeTime = 600
	}
}

func (m *GCChecker) Run(ctx context.Context) {

	klog.CtxInfof(ctx, "start db gc checker with opts %+v", *m.opts)
	ticker := time.NewTicker(time.Duration(m.opts.GCInterval) * time.Second)
	for {
		select {
		case <-ticker.C:
			// for config change
			if !m.opts.GCEnabled {
				klog.CtxInfof(ctx, "gc checker unenabled", *m.opts)
				return
			}

			if !m.leaderChecker.IsLeader(ctx) {
				continue
			}

			lastPoint, err := Uint64(m.kvClient.GetKVClient().Get(ctx, jobEncodeGCPointKey()))
			if err != nil {
				klog.Errorf("load last safe point failed, error: %s", err.Error())
				continue
			}

			newPoint, err := m.getNewPoint(time.Duration(m.opts.GCSafePointLifeTime) * time.Second)
			if err != nil {
				klog.Errorf("get db safe point for gc error: %s", err.Error())
				continue
			}

			lastPointTime := time.Unix(int64(lastPoint), 0)
			if newPoint.Sub(lastPointTime) < time.Duration(m.opts.GCSafePointLifeTime)*time.Second {
				klog.CtxWarnf(ctx, "do not need run gc this time, %d seconds past after last gc", newPoint.Sub(lastPointTime)/time.Second)
				continue
			}

			safePoint := oracle.ComposeTS(oracle.GetPhysical(newPoint), 0)
			err = m.runGC(ctx, safePoint)
			if err != nil {
				klog.CtxErrorf(ctx, "run gc failed, error: %s", err.Error())
				continue
			}

			err = m.kvClient.GetKVClient().Put(ctx, jobEncodeGCPointKey(), PutInt64(newPoint.Unix()))
			if err != nil {
				klog.CtxErrorf(ctx, "save safe point failed, error: %s", err.Error())
				continue
			}

			klog.CtxInfof(ctx, "gc checker done, new safe point %+v", newPoint)
		case <-ctx.Done():
			return
		}
	}
}

func (m *GCChecker) getNewPoint(ttl time.Duration) (time.Time, error) {
	currentTS, err := m.kvClient.GetTxnKVClient().KVStore.CurrentTimestamp(oracle.GlobalTxnScope)
	if err != nil {
		return time.Time{}, err
	}
	physical := oracle.ExtractPhysical(currentTS)
	sec, nsec := physical/1e3, (physical%1e3)*1e6
	now := time.Unix(sec, nsec)
	safePoint := now.Add(-ttl)

	return safePoint, nil
}

// runGc
// resolve lock on the whole TiKV cluster.
// notice: the range is unbounded.
func (m *GCChecker) runGC(ctx context.Context, safePoint uint64) (err error) {
	_, err = m.kvClient.GetTxnKVClient().GC(ctx, safePoint, tikvClient.WithConcurrency(m.opts.GCConcurrency))
	return
}
