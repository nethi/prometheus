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

package zpacker

import (
	"container/list"
	"sync"
	"time"

	"github.com/go-kit/kit/log"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/ztypes"
)

const (
	INITIAL_samples_alloc = 1000
)

type ZPackInsCache struct {
	pSamples []ztypes.StWbSample // Cached samples.
	pHashes  []uint64            // Hash of all the labels (well yolostring of buffer), one for each sample. Used for quick compare to detect change.
	cHashes  []uint64            // Hash of all the labels (well yolostring of buffer), one for each sample. Used for quick compare to detect change.
	cSamples []ztypes.StWbSample // Current samples.
	nok      int                 // Number of samples that have same metadata as the last iteration.
	nadded   int                 // Number of samples added.
	dflttm   int64               // Default sample time to use.
}

type ZPackIns struct {
	l           log.Logger    // Logger
	instance    string        // Instance name.
	isSkipBatch bool          // Should we skip the current batch?
	nrDropped   int           // Number of batches dropped.
	lk          sync.Mutex    // Lock that protects fields in the ZPackInsInfo.
	refCnt      int32         // Ref count (protected by lk).
	activeBlobs int           // Active blob count (protected by lk).
	ioBlobs     int           // Active blobs that were sent to queue manager, waiting on completion.
	cGen        int64         // Current generation number.
	sealedBlobs *list.List    // List of ZBlobTuple(s). (protected by lk)
	cache       ZPackInsCache // Cached samples of this instance.
	qmgrinst    int           // Queuemanger registration instance.
	mps         int32         // Minimum polling seconds allowed.
}

// MPS : Minimum polling seconds allowed from the remote server.
// If the remote server says a target cannot be polled faster than this, then
// we need to enforce that here. This is basically to not load the backend server
// with malicious users that want to put too much load on the remote server.
func (zp *ZPackIns) GetMPS() int32 {
	return zp.mps
}

// Starts a new batch of samples from a target. Called at the start of the every scrape.
func (zp *ZPackIns) StartBatch(dt int64) {
	zp.startBatch(dt)
}

// Adds a scraped sample.
func (zp *ZPackIns) AddSample(ls labels.Labels, hash uint64, t int64, v float64, help string, mtype string, unit string) {
	zp.addSample(ls, hash, t, v, help, mtype, unit)
}

// Finishes the batch of samples from a target. Called at the end of every scrape.
func (zp *ZPackIns) CommitBatch(olabels []string) {
	zp.commitBatch(olabels)
}

// Finishes the batch of samples from a target. Called at the end of every scrape, if there is an error.
func (zp *ZPackIns) RollbackBatch() {
	zp.rollbackBatch()
}

// This is the callback comes from the zqueue manager, saying a blob is sent.
func (zp *ZPackIns) SendCompleted(err error, mps int) {
	zp.sendCompleted(err, mps)
}

func (zp *ZPackIns) Put() {
	zp.putInstance()
}

type GlInfo struct {
	inss                        map[string]*ZPackIns // Map indexed by instance.
	lk                          sync.Mutex           // Lock.
	MaxQueuedScrapesPerInstance int                  // Max queued scrapes per instance.
	zdn                         string               // Zebrium deployment name.
}

var gli GlInfo

func init() {
	gli = GlInfo{inss: make(map[string]*ZPackIns)}
}

func Get(instance string, l log.Logger, interval time.Duration) (*ZPackIns, error) {
	return getInstance(instance, l, interval)
}

func InitPacker(maxQueuedScrapesPerInstance int, zdn string) {
	gli.MaxQueuedScrapesPerInstance = maxQueuedScrapesPerInstance
	gli.zdn = zdn
}

func GetDeploymentName() string {
	return gli.zdn
}
