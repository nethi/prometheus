// Copyright 2019 Zebrium Inc
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zstorage

import (
	"context"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

type zstore struct {
	logger log.Logger
}

// NewZStore returns a new dummy Storage.
func NewZStore(logger log.Logger) storage.Storage {
	return &zstore{
		logger: logger,
	}
}

// StartTime implements the Storage interface.
func (f *zstore) StartTime() (int64, error) {
	return int64(0), nil
}

func (f *zstore) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	//return nil, fmt.Errorf("Not supported")
	return storage.NoopQuerier(), nil
}

func (f *zstore) Appender() (storage.Appender, error) {
	return &zstoreAppender{
		logger: f.logger,
	}, nil
}

// Close closes the storage and all its underlying resources.
func (f *zstore) Close() error {
	return nil
}

// zstoreAppender implements Appender.
type zstoreAppender struct {
	logger log.Logger
}

func (f *zstoreAppender) Add(l labels.Labels, t int64, v float64) (uint64, error) {
	return uint64(0), nil
}

func (f *zstoreAppender) AddFast(l labels.Labels, ref uint64, t int64, v float64) error {
	return nil
}

func (f *zstoreAppender) Commit() (err error) {
	return nil
}

func (f *zstoreAppender) Rollback() (err error) {
	return nil
}
