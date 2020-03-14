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
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"

	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/prometheus/prometheus/zqmgr"
	"github.com/prometheus/prometheus/ztypes"
)

// Allocates a new instance.
func allocateInstance(instance string, l log.Logger) *ZPackIns {

	ni := &ZPackIns{l: l, instance: instance, isSkipBatch: false, refCnt: 0, activeBlobs: 0, ioBlobs: 0, cGen: time.Now().UnixNano(),
		sealedBlobs: list.New(), mps: int32(0)}

	ni.reinitCache()
	return ni
}

// Get the new zpacker instance for a given scrape isntance.
func getInstance(instance string, l log.Logger, interval time.Duration) (*ZPackIns, error) {
	var ni *ZPackIns

	for {

		gli.lk.Lock()
		ret, isok := gli.inss[instance]
		if !isok && ni != nil {
			ret = ni
			gli.inss[instance] = ni
			isok = true
		}
		if isok {
			if atomic.LoadInt32(&ret.refCnt) != int32(0) {
				err := fmt.Errorf("Invalid refcount %d for instance %s", ret.refCnt, instance)
				level.Warn(l).Log("msg", err)
				return nil, err
			}
			atomic.AddInt32(&ret.refCnt, 1)
			ret.cGen = time.Now().UnixNano()
			gli.lk.Unlock()
			ret.qmgrinst = zqmgr.RegisterZPackerInstance(interval)
			return ret, nil
		}
		gli.lk.Unlock()

		ni = allocateInstance(instance, l)
	}

}

// Puts the zpacker instance. every get needs to do put at the end of its use (i.e when target goes away).
func (zp *ZPackIns) putInstance() {
	zqmgr.UnregisterZPackerInstance(zp.qmgrinst)

	gli.lk.Lock()

	lzp, isok := gli.inss[zp.instance]
	if isok && lzp == zp {
		if atomic.LoadInt32(&zp.refCnt) != int32(1) {
			level.Warn(zp.l).Log("msg", fmt.Sprintf("Invalid refcount %d in putinstance on %s", zp.refCnt, zp.instance))
			return
		}
		atomic.AddInt32(&zp.refCnt, -1)
		gli.lk.Unlock()

		zp.lk.Lock()
		if zp.activeBlobs == 0 {
			zp.lk.Unlock()
			gli.lk.Lock()
			delete(gli.inss, zp.instance)
			gli.lk.Unlock()
		} else {
			zp.lk.Unlock()
		}
	} else {
		gli.lk.Unlock()
		level.Warn(zp.l).Log("msg", fmt.Sprintf("Invalid putinstance on %s isok %v", zp.instance, isok))
	}
}

// zpacker maintains the case of the last scrape, so that it can create the incremental blob.
// This reinits the cache.
func (zp *ZPackIns) reinitCache() {
	zp.cache.pSamples = make([]ztypes.StWbSample, 0)
	zp.cache.cSamples = make([]ztypes.StWbSample, 0)
	zp.cache.pHashes = make([]uint64, 0)
	zp.cache.cHashes = make([]uint64, 0)
	zp.cache.nok = 0
	zp.cache.nadded = 0
	zp.cGen = time.Now().UnixNano()
}

// Starts a new batch of samples from a target. Called at the start of the every scrape.
func (zp *ZPackIns) startBatch(dt int64) {
	nr := INITIAL_samples_alloc

	if len(zp.cache.pSamples) != 0 {
		nr = len(zp.cache.pSamples)
	}
	if len(zp.cache.cSamples) != nr {
		zp.cache.cSamples = make([]ztypes.StWbSample, 0, nr)
	}
	if len(zp.cache.cHashes) != nr {
		zp.cache.cHashes = make([]uint64, 0, nr)
	}
	zp.cache.nok = 0
	zp.cache.nadded = 0
	zp.cache.dflttm = dt

	lastSkipBatch := zp.isSkipBatch
	zp.isSkipBatch = false
	zp.lk.Lock()
	if zp.sealedBlobs.Len() >= gli.MaxQueuedScrapesPerInstance {
		zp.isSkipBatch = true
	}
	zp.lk.Unlock()

	if lastSkipBatch != zp.isSkipBatch && zp.nrDropped > 0 {
		level.Info(zp.l).Log("msg", fmt.Sprintf("Resuming stats collection for instance %s after dropping %d batches", zp.instance, zp.nrDropped))
	}

	if zp.isSkipBatch {
		zp.nrDropped++
	} else {
		zp.nrDropped = 0
	}
}

// Adds a scraped sample.
func (zp *ZPackIns) addSample(ls labels.Labels, hash uint64, t int64, v float64, help string, mtype string, unit string) {
	if zp.isSkipBatch {
		return
	}

	if len(zp.cache.pHashes) > zp.cache.nadded && zp.cache.pHashes[zp.cache.nadded] == hash {
		zp.cache.nok++
	}

	ws := ztypes.StWbSample{Idx: int32(zp.cache.nadded), Ls: ls, Help: help, Type: mtype, Ts: t, Val: v}

	if len(zp.cache.cHashes) <= zp.cache.nadded {
		zp.cache.cHashes = append(zp.cache.cHashes, hash)
		zp.cache.cSamples = append(zp.cache.cSamples, ws)
	} else {
		zp.cache.cHashes[zp.cache.nadded] = hash
		zp.cache.cSamples[zp.cache.nadded] = ws
	}
	zp.cache.nadded++
}

// Is the current scraped scample same as the last time we scraped this?
func (zp *ZPackIns) isSampleSame(idx int) bool {
	if zp.cache.cHashes[idx] == zp.cache.pHashes[idx] &&
		((zp.cache.cSamples[idx].Val == zp.cache.pSamples[idx].Val) || (math.IsNaN(zp.cache.cSamples[idx].Val) && math.IsNaN(zp.cache.pSamples[idx].Val))) &&
		zp.cache.cSamples[idx].Ts == zp.cache.dflttm {
		return true
	}
	return false
}

// Creates an incremental blob.
func (zp *ZPackIns) createIncrBlob(olabels []string) *ztypes.StWbIReq {
	wr := &ztypes.StWbIReq{Instance: zp.instance, NSamples: zp.cache.nadded, IsIncr: true, Gen: zp.cGen, GTs: zp.cache.dflttm}

	nr := 0
	for i := 0; i < zp.cache.nadded; i++ {
		if !zp.isSampleSame(i) {
			nr++
		}
	}

	samples := make([]ztypes.StWbSample, nr)
	cidx := 0
	for i := 0; i < zp.cache.nadded; i++ {
		if !zp.isSampleSame(i) {
			s := &samples[cidx]
			s.Idx = int32(i)
			s.Ts = zp.cache.cSamples[i].Ts - zp.cache.pSamples[i].Ts
			s.Val = zp.cache.cSamples[i].Val - zp.cache.pSamples[i].Val
			cidx++
		}
	}

	wr.Samples = samples
	return wr
}

// Creates a full blob.
func (zp *ZPackIns) createFullBlob(olabels []string) *ztypes.StWbIReq {
	wr := &ztypes.StWbIReq{Instance: zp.instance, NSamples: zp.cache.nadded, IsIncr: false, Gen: zp.cGen, GTs: zp.cache.dflttm, OLabels: olabels}

	samples := make([]ztypes.StWbSample, zp.cache.nadded)
	for i := 0; i < zp.cache.nadded; i++ {
		samples[i] = zp.cache.cSamples[i]
	}

	wr.Samples = samples
	return wr
}

// Finishes the batch of samples from a target. Called at the end of every scrape.
// As part of commit, we crate incremental and full blob and queue them to queue manager for sending
// to remote server.
func (zp *ZPackIns) commitBatch(olabels []string) {
	if zp.isSkipBatch {
		return
	}

	if zp.cache.nadded == 0 {
		level.Info(zp.l).Log("msg", fmt.Sprintf("Instance %s, no samples added in commit", zp.instance))
		zp.reinitCache()
		return
	}

	tpl := &ztypes.ZBlobTuple{Cb: zp, SkipWaitQ: false}

	// Create the incremental blob, if metadata is same and the number of counters are same.
	if zp.cache.nok == zp.cache.nadded {
		tpl.IncrBlob = zp.createIncrBlob(olabels)
		tpl.FlBlob = zp.createFullBlob(olabels)
		zp.cache.pSamples, zp.cache.cSamples = zp.cache.cSamples, zp.cache.pSamples
		zp.cache.pHashes, zp.cache.cHashes = zp.cache.cHashes, zp.cache.pHashes
	} else {
		// Incremental blob is not allowed here. (change detected)
		zp.cGen = time.Now().UnixNano()
		tpl.FlBlob = zp.createFullBlob(olabels)
		zp.cache.pSamples = zp.cache.cSamples
		zp.cache.pHashes = zp.cache.cHashes
		zp.cache.cSamples = nil
		zp.cache.cHashes = nil
	}
	zp.cGen++

	send := true

	zp.lk.Lock()
	if zp.activeBlobs > 0 {
		zp.sealedBlobs.PushBack(tpl)
		send = false
	} else {
		zp.ioBlobs++
	}
	zp.activeBlobs++
	nrActive := zp.activeBlobs
	zp.lk.Unlock()

	flushThreshold := gli.MaxQueuedScrapesPerInstance / 2
	if flushThreshold < 2 {
		flushThreshold = 2
	}

	if send {
		zp.sendBlob(tpl)
	} else if nrActive > flushThreshold {
		zqmgr.FlushWaitQReq()
	}
}

// Finishes the batch of samples from a target. Called at the end of every scrape, if there is an error.
// We just abort sending batch.
func (zp *ZPackIns) rollbackBatch() {
	zp.cache.nok = 0
	zp.cache.nadded = 0
}

func (zp *ZPackIns) sendBlob(tpl *ztypes.ZBlobTuple) {
	err := zqmgr.EnqueueBlobTuple(tpl)
	if err != nil {
		level.Warn(zp.l).Log("msg", fmt.Sprintf("Failed to enqueue tuple for instance %s err %s", zp.instance, err))
	}
}

func (zp *ZPackIns) sendBlobs(tpls *list.List) {
	err := zqmgr.EnqueueBlobTuples(tpls)
	if err != nil {
		level.Warn(zp.l).Log("msg", fmt.Sprintf("Failed to enqueue %d tuple for instance %s err %s", tpls.Len(), zp.instance, err))
	}
}

// This is the callback comes from the zqueue manager, saying a blob is sent.
// We send the next set of blobs, if there are any.
func (zp *ZPackIns) sendCompleted(err error, mps int) {
	if err != nil {
		level.Debug(zp.l).Log("msg", fmt.Sprintf("Failed to send tuple, instance %s, err %s", zp.instance, err))
	}

	if mps >= 0 {
		atomic.StoreInt32(&zp.mps, int32(mps))
	}
	var nreqs *list.List

	zp.lk.Lock()
	if zp.activeBlobs < 1 || zp.ioBlobs < 1 {
		level.Warn(zp.l).Log("msg", fmt.Sprintf("Invalid active/io blobs active %d ioblobls %d for instance %s", zp.activeBlobs, zp.ioBlobs, zp.instance))
	} else {
		zp.activeBlobs--
		zp.ioBlobs--
	}

	if zp.sealedBlobs.Len() > 0 && zp.ioBlobs == 0 {
		nreqs = zp.sealedBlobs
		zp.sealedBlobs = list.New()
		zp.ioBlobs += nreqs.Len()
		if zp.activeBlobs != zp.ioBlobs {
			level.Warn(zp.l).Log("msg", fmt.Sprintf("Mismatched active/io blobs active %d ioblobls %d for instance %s", zp.activeBlobs, zp.ioBlobs, zp.instance))
		}
	}
	zp.lk.Unlock()

	if nreqs != nil {
		zp.sendBlobs(nreqs)
	} else if zp.activeBlobs == 0 && atomic.LoadInt32(&zp.refCnt) == int32(0) {
		gli.lk.Lock()
		lzp, isok := gli.inss[zp.instance]
		if isok && lzp == zp {
			delete(gli.inss, zp.instance)
		}
		gli.lk.Unlock()
	}
}
