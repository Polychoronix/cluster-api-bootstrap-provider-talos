// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	"context"
	"github.com/google/uuid"
	"sync"
	"time"
)

type operationRunner struct {
	mu      sync.Mutex
	ctx     context.Context
	running map[string]bool
	issuer  string
	wg      sync.WaitGroup
}

func newOperationRunner() *operationRunner {
	return &operationRunner{running: map[string]bool{}, issuer: uuid.NewString()}
}
func (r *operationRunner) start(key string, run func(context.Context)) {
	r.mu.Lock()
	if r.ctx == nil || r.running[key] {
		r.mu.Unlock()
		return
	}
	r.running[key] = true
	r.wg.Add(1)
	ctx := r.ctx
	r.mu.Unlock()
	go func() {
		defer r.wg.Done()
		defer func() { r.mu.Lock(); delete(r.running, key); r.mu.Unlock() }()
		run(ctx)
	}()
}

// Start binds background operations to the manager, not one HTTP hook request.
func (h *Handler) Start(ctx context.Context) error {
	h.runner.mu.Lock()
	h.runner.ctx = ctx
	h.runner.mu.Unlock()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			h.runner.wg.Wait()
			return nil
		case <-ticker.C:
			h.cleanupReceipts(ctx)
		}
	}
}
func (h *Handler) NeedLeaderElection() bool { return false }
