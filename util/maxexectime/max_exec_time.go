// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package maxexectime

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// InterruptableQuery is the interface that for queries to be monitored
type InterruptableQuery interface {
	MaxExecTimeExceeded() bool
	CancelOnTimeout()
	QueryID() uint32
}

// Monitor is the object that monitors queries
type Monitor struct {
	mu      sync.RWMutex
	sctx    sessionctx.Context
	exitCh  chan struct{}
	queries map[uint32]InterruptableQuery
}

// AddQuery adds a query to be monitored
func (mntr *Monitor) AddQuery(query InterruptableQuery) error {
	id := query.QueryID()
	mntr.mu.Lock()
	defer mntr.mu.Unlock()
	if mntr.queries[id] != nil {
		logutil.Logger(context.Background()).Info("duplicate query", zap.Uint32("id:", id))
		return errors.New("duplicate query id")
	}
	mntr.queries[query.QueryID()] = query
	return nil
}

// RemoveQuery removes a query
func (mntr *Monitor) RemoveQuery(query InterruptableQuery) {
	mntr.mu.Lock()
	defer mntr.mu.Unlock()
	id := query.QueryID()
	mntr.queries[id] = nil
}

func (mntr *Monitor) checkAll() {
	mntr.mu.RLock()
	queries := mntr.queries
	mntr.mu.RUnlock()

	for _, query := range queries {
		if query.MaxExecTimeExceeded() {
			logutil.Logger(context.Background()).Info("cancel query")
			query.CancelOnTimeout()
			mntr.RemoveQuery(query)
		}
	}
}

// Run is the main func
func (mntr *Monitor) Run() {
	logutil.Logger(context.Background()).Info("Monitor runs")
	//TODO: use configed param
	ticker := time.NewTicker(time.Duration(10) * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			mntr.checkAll()
		case <-mntr.exitCh:
			logutil.Logger(context.Background()).Info("Monitor exits")
			return
		}
	}
}

// NewMaxExecTimeMonitor creates a Monitor
func NewMaxExecTimeMonitor(sctx sessionctx.Context, exitCh chan struct{}) *Monitor {
	return &Monitor{
		sctx:    sctx,
		exitCh:  exitCh,
		queries: make(map[uint32]InterruptableQuery),
	}
}
