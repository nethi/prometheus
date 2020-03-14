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

// ZQueue manager:
//  Responsible for sending blobs to remote server.
//  Main state machine thread that takes care of all the decisions and the queue.
//  main State machine, determines the number of blobs coelesce in a single http request.
//  Maintains a qeueue depth of active http requests allowed.
//  spawns a go thread for each active HTTP request. And this go thread takes care of retries.

package zqmgr

import (
	"compress/gzip"
	"container/list"
	"context"
	"crypto/tls"
	"encoding/gob"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"

	"github.com/prometheus/prometheus/ztypes"
)

const (
	QMGR_reqch_len         = 256  // Channel length, beyond which caller will block.
	QMGR_http_timeout_secs = 300  // HTTP request timeout in seconds.
	QMGR_scrape_min_secs   = 2    // Minimum scraping seconds allowed.
	QMGR_scrape_max_secs   = 60   // Max scraping seconds.
	QMGR_bids_alloc_len    = 1000 // Allocation of vector in batches of this will happen.
)

type QMgrState int

// Queue manager state machine for handling server being unavailble for short period of time.
// In general queue manager will be in the normal state. This means we are able to send our blobs to server just fine.
//
// The moment we get either connection refused or 50[0-3] errors, we do not want to drop the stats that we collected,
// As this could happen because of remote server going through a short un-availbility phase, because of software
// upgrades or other maintenance reasons.
// In this case, keeping stats that we collect in in-memory is not possible, as they won't fit. So we start
// spilling stats on the disk. When the server comes back up, we will replay stats in sequence from disk.
//
// QSTATE_normal: Normal state, no disk spilling. sending all in-memory stats to remote server.
// QSTATE_buffer: remote server not available, All the stats are being spilled to disk.
// QSTATE_replay: remote server just came back up, replaying stats from on-disk. new stats
//                that are being scraped still go to disk.
// QSTATE_replay2: Once the initial set of stats are replayed, we replay the stats
//                 that came in, while we replayed in QSTATE_replay state. In this state,
//                 new stats that are being scraped will be buffered in in-memory.
//
// State transitions:
//  QSTATE_normal  -> QSTATE_buffer  (when we goet error for server)
//  QSTATE_buffer  -> QSTATE_replay  (when the server gets back online)
//  QSTATE_replay  -> QSTATE_buffer  (gets an error from server while replaying)
//  QSTATE_replay  -> QSTATE_replay2 (First replay phase is done)
//  QSTATE_replay2 -> QSTATE_normal  (Done with phase 2 replay)
//  QSTATE_replay2 -> QSTATE_buffer  (gets an error from server while replaying in phase 2)
const (
	QSTATE_normal QMgrState = iota
	QSTATE_buffer
	QSTATE_replay
	QSTATE_replay2
)

func (s QMgrState) String() string {
	return [...]string{"NORMAL", "BUFFER", "REPLAY", "REPLAY2"}[s]
}

type QMgrInfo struct {
	zurl string          // Zebrium URL to use for sending requests.
	ztkn string          // Zebrium token to use for sending requests.
	ztr  *http.Transport // Settings for TLS

	l log.Logger // Logger.

	flushWaitQCh       chan bool               // CHannel that listens for flush waitq requests.
	reqCh              chan *ztypes.ZBlobTuple // Request channel.
	reqsCh             chan *list.List         // Requests channel.
	webSenderRespCh    chan *workerReq         // Response channel from the worker.
	bufferWriteRespCh  chan *workerReq         // Response channel from the buffered writer worker.
	bufferReplayRespCh chan []uint64           // Response channel from buffered replay worker.
	healthWorkerRespCh chan bool               // Response channel from server health worker.

	stopping      bool               // Are we in the process of shutting down?
	exitMgrCh     chan string        // Manger exit channel.
	exitMgrWaiter sync.WaitGroup     // Waitq for manager to be stopped
	mgrCtxt       context.Context    // Context used to cancel during exit.
	cancelFnc     context.CancelFunc // Cancel function.

	stopAllWorkers bool // Should the worker threads be stopped?

	maxQD int // Max worker queue depth.
	curQD int // Current worker queue depth.

	bufWaitSecs       int // buffering timeout in seconds.
	maxReqsToCoelesce int // Max requests to coelesce in a single HTTPS request.
	wakeupInterval    int // How frequently our manager thread wakeup.

	reqsQ *list.List // Requests waiting to be worked on. (runq)
	waitQ *list.List // Requests waiting in the buffered queue.

	nextWakeUpTime time.Time // Next wakeup time for moving elements from waitq to runq.

	scrapeIntervalsLk sync.Mutex // locks that protects the scrapeIntervals slice.
	scrapeIntervals   []int      // Scrape intervals indexed in seconds.
	minScrapeSecs     int32      // Minimum scrape seconds from any target.

	qState   QMgrState // Q Mode state.
	bufDir   string    // buffer directory.
	cwuid    uint64    // current worker unique monotonically increasing id.
	bids     []uint64  // buffered ids.
	doReplay bool      // Should we schedule replay worker.

	maxBufs int // Max buffers allowed to write on disk.
}

// Remote server can limit the frequency of scraping, for malicious users.
func adjustScrapeSecs(secs int) int {
	if secs < QMGR_scrape_min_secs {
		secs = QMGR_scrape_min_secs
	}
	if secs > QMGR_scrape_max_secs {
		secs = QMGR_scrape_max_secs
	}
	return secs
}

func GetQMgrInfo(url string, insecureSSL bool, token string, bufDir string, mqd int,
	misToCoelesce int, bufWaitSecs int, maxQueuedScrapesPerInstance int, maxBufs int, l log.Logger) *QMgrInfo {

	bufWaitSecs = adjustScrapeSecs(bufWaitSecs)

	qi := &QMgrInfo{zurl: url, ztkn: token, l: l,
		reqCh:              make(chan *ztypes.ZBlobTuple, QMGR_reqch_len),
		reqsCh:             make(chan *list.List, QMGR_reqch_len),
		flushWaitQCh:       make(chan bool, QMGR_reqch_len),
		webSenderRespCh:    make(chan *workerReq, mqd),
		bufferWriteRespCh:  make(chan *workerReq, mqd),
		bufferReplayRespCh: make(chan []uint64, 1),
		healthWorkerRespCh: make(chan bool, 1),
		stopping:           false,
		exitMgrCh:          make(chan string, 1),
		stopAllWorkers:     false,
		maxQD:              mqd,
		curQD:              0,
		bufWaitSecs:        bufWaitSecs,
		wakeupInterval:     bufWaitSecs,
		maxReqsToCoelesce:  misToCoelesce,
		reqsQ:              list.New(),
		waitQ:              list.New(),
		scrapeIntervals:    make([]int, QMGR_scrape_max_secs),
		minScrapeSecs:      2,
		qState:             QSTATE_normal,
		cwuid:              uint64(0),
		bids:               make([]uint64, 0, QMGR_bids_alloc_len),
		maxBufs:            maxBufs,
	}

	qi.nextWakeUpTime = time.Now().Add(time.Duration(qi.wakeupInterval) * time.Second)

	if insecureSSL && url[:8] == "https://" {
		qi.ztr = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	}

	qi.bufDir = filepath.Join(bufDir, "zstats")
	if _, err := os.Stat(qi.bufDir); err == nil || !os.IsNotExist(err) {
		if err := os.RemoveAll(qi.bufDir); err != nil {
			panic(fmt.Errorf("Could not remove dir %s err %s\n", qi.bufDir, err))
		}
	}
	if err := os.MkdirAll(qi.bufDir, 0755); err != nil {
		panic(fmt.Errorf("Cannot create directory %s, err %s\n", qi.bufDir, err))
	}

	qi.exitMgrWaiter.Add(1)
	level.Info(l).Log("msg", fmt.Sprintf("Zebrium qmgr started with QD: %d bufTimeoutSecs: %d maxReqsToCoelesce: %d maxQueuedScrapesPerInstance: %d bufDir %s maxBufs %d", mqd, bufWaitSecs, misToCoelesce, maxQueuedScrapesPerInstance, bufDir, maxBufs))
	return qi
}

func (qi *QMgrInfo) initQMgr() error {

	go qi.mgrThread()
	return nil
}

// Called with scrapeIntervalsLk lock.
func (qi *QMgrInfo) fixMinScrapeSecs() {
	min := int32(QMGR_scrape_max_secs)

	for i := QMGR_scrape_min_secs - 1; i < QMGR_scrape_max_secs; i++ {
		if qi.scrapeIntervals[i] > 0 {
			min = int32(i + 1)
			break
		}
	}

	atomic.StoreInt32(&qi.minScrapeSecs, min)
}

func (qi *QMgrInfo) addScrapeInterval(secs int) int {
	secs = adjustScrapeSecs(secs)

	qi.scrapeIntervalsLk.Lock()
	defer qi.scrapeIntervalsLk.Unlock()
	qi.scrapeIntervals[secs-1]++

	qi.fixMinScrapeSecs()
	return secs
}

func (qi *QMgrInfo) removeScrapeInterval(secs int) {
	secs = adjustScrapeSecs(secs)

	qi.scrapeIntervalsLk.Lock()
	defer qi.scrapeIntervalsLk.Unlock()
	if qi.scrapeIntervals[secs-1] > 0 {
		qi.scrapeIntervals[secs-1]--
		qi.fixMinScrapeSecs()
	}
}

func (qi *QMgrInfo) enqueueBlobTuple(req *ztypes.ZBlobTuple) error {
	if !qi.stopping {
		qi.reqCh <- req
	}
	return nil
}

func (qi *QMgrInfo) enqueueBlobTuples(reqs *list.List) error {
	if !qi.stopping {
		qi.reqsCh <- reqs
	}
	return nil
}

func (qi *QMgrInfo) flushWaitQReq() {
	if !qi.stopping {
		qi.flushWaitQCh <- true
	}
}

func (qi *QMgrInfo) stopQMgr() {
	level.Info(qi.l).Log("msg", "Stopping the manager thread")
	qi.exitMgrCh <- "exit"
	close(qi.exitMgrCh)
	qi.exitMgrWaiter.Wait()
}

func (qi *QMgrInfo) doStopQMgr() {
	qi.stopping = true

	qi.stopAllWorkers = true
	qi.cancelFnc()

	for qi.curQD != 0 || qi.qState != QSTATE_normal {
		if qi.qState == QSTATE_replay && qi.doReplay {
			break
		}
		select {
		case resp := <-qi.webSenderRespCh:
			qi.processWebSenderResponse(resp)
		case resp := <-qi.bufferWriteRespCh:
			qi.processBufferWriterResponse(resp)
		case <-qi.healthWorkerRespCh:
			qi.processHealthWorkerResponse()
		case bresp := <-qi.bufferReplayRespCh:
			qi.processBufferReplayResponse(bresp)
		}
	}

	level.Info(qi.l).Log("msg", "Zebrium QManager exiting")
	qi.exitMgrWaiter.Done()
}

func (qi *QMgrInfo) processRequest(req *ztypes.ZBlobTuple) {
	if req.SkipWaitQ {
		qi.reqsQ.PushBack(req)
	} else {
		if qi.waitQ.Len() == 0 {
			qi.setNextWakeUpTime()
		}
		qi.waitQ.PushBack(req)
	}
}

func (qi *QMgrInfo) processRequests(reqs *list.List) {
	for reqs.Len() > 0 {
		el := reqs.Front()
		reqs.Remove(el)
		req := el.Value.(*ztypes.ZBlobTuple)
		qi.processRequest(req)
	}
}

func (qi *QMgrInfo) moveAllToRunQ() {
	if qi.waitQ.Len() > 0 {
		qi.reqsQ.PushBackList(qi.waitQ)
		qi.waitQ = list.New()
	}
}

func (qi *QMgrInfo) setNextWakeUpTime() {
	wakeupInterval := int(atomic.LoadInt32(&qi.minScrapeSecs))
	if wakeupInterval < qi.bufWaitSecs {
		wakeupInterval = qi.bufWaitSecs
	}
	qi.nextWakeUpTime = time.Now().Add(time.Duration(wakeupInterval) * time.Second)
}

func (qi *QMgrInfo) getInstanceFromZBlobTuple(tpl *ztypes.ZBlobTuple) string {
	if tpl.FlBlob != nil {
		return tpl.FlBlob.Instance
	}
	if tpl.IncrBlob != nil {
		return tpl.IncrBlob.Instance
	}
	return ""
}

// Main thread state mahine logic.
func (qi *QMgrInfo) mgrStateMachine() {

	if qi.qState == QSTATE_replay2 {
		// If we are in replay2 state, we would like to leave the queue as is, as replay2 should not take lot of time.
		return
	}

	// Schedule the replay worker, if needed.
	if qi.curQD == 0 && qi.doReplay {
		bids := qi.bids
		qi.bids = make([]uint64, 0, QMGR_bids_alloc_len)
		qi.doReplay = false
		go qi.replayWorker(bids)
	}

	for qi.curQD < qi.maxQD && qi.reqsQ.Len() > 0 {

		nr := qi.reqsQ.Len()
		if qi.maxReqsToCoelesce > 0 && nr > qi.maxReqsToCoelesce {
			nr = qi.maxReqsToCoelesce
		}

		qi.cwuid++
		wreq := &workerReq{reqs: make([]*ztypes.ZBlobTuple, nr), uid: qi.cwuid, qi: qi, err: nil, shdBuffer: false}
		for i := 0; i < nr; i++ {
			el := qi.reqsQ.Front()
			wreq.reqs[i] = el.Value.(*ztypes.ZBlobTuple)
			qi.reqsQ.Remove(el)
		}

		// Take more, if the last instance is same as the next one.
		// This is needed, so that we never send the same instance with more than one parallel POST requests.
		linstance := qi.getInstanceFromZBlobTuple(wreq.reqs[len(wreq.reqs)-1])
		if qi.reqsQ.Len() > 0 {
			for qi.reqsQ.Len() > 0 {
				cinstance := qi.getInstanceFromZBlobTuple(qi.reqsQ.Front().Value.(*ztypes.ZBlobTuple))
				if cinstance != linstance {
					break
				}

				el := qi.reqsQ.Front()
				wreq.reqs = append(wreq.reqs, el.Value.(*ztypes.ZBlobTuple))
				qi.reqsQ.Remove(el)
			}
		}

		qi.curQD++
		if qi.qState == QSTATE_normal {
			go wreq.webSenderThread()
		} else if qi.qState == QSTATE_buffer || qi.qState == QSTATE_replay {
			drop := false
			if len(qi.bids) >= qi.maxBufs {
				level.Warn(qi.l).Log("msg", fmt.Sprintf("Dropping stats as disk buffer is full %d", len(qi.bids)))
				drop = true
			} else {
				qi.bids = append(qi.bids, wreq.uid)
			}
			go wreq.bufferWriteWorker(drop, true /* docb */)
		}
		level.Info(qi.l).Log("msg", fmt.Sprintf("QMGRSTATS: state %s inq %d waitq %d cur_qd %d bids %d", qi.qState, qi.reqsQ.Len(), qi.waitQ.Len(), qi.curQD, len(qi.bids)))
	}

}

func (qi *QMgrInfo) processBufferWriterResponse(w *workerReq) {
	if w.err != nil {
		level.Warn(qi.l).Log("msg", fmt.Sprintf("bufferwriter response err: %s", w.err))
	}

	if qi.curQD == 0 {
		level.Error(qi.l).Log("msg", fmt.Sprintf("invalid current queue depth %d", qi.curQD))
		return
	}
	qi.curQD--
	level.Info(qi.l).Log("msg", fmt.Sprintf("QMGRSTATS: state %s inq %d waitq %d cur_qd %d bids %d", qi.qState, qi.reqsQ.Len(), qi.waitQ.Len(), qi.curQD, len(qi.bids)))
}

func (qi *QMgrInfo) processWebSenderResponse(w *workerReq) {
	if w.err != nil {
		level.Warn(qi.l).Log("msg", fmt.Sprintf("Websender response err: %s", w.err))
		if w.shdBuffer && (qi.qState == QSTATE_normal || qi.qState == QSTATE_buffer) {
			if qi.qState == QSTATE_normal {
				level.Warn(qi.l).Log("msg", fmt.Sprintf("STATE change to %s from %s", QSTATE_buffer, qi.qState))
				qi.qState = QSTATE_buffer
				go qi.serverHealthWorker()
			}
			qi.bids = append(qi.bids, w.uid)
			go w.bufferWriteWorker(false /* drop */, false /* docb */)
			return
		}
	}

	if qi.curQD == 0 {
		level.Error(qi.l).Log("msg", fmt.Sprintf("invalid current queue depth %d", qi.curQD))
		return
	}
	qi.curQD--
	level.Info(qi.l).Log("msg", fmt.Sprintf("QMGRSTATS: state %s inq %d waitq %d cur_qd %d bids %d", qi.qState, qi.reqsQ.Len(), qi.waitQ.Len(), qi.curQD, len(qi.bids)))
}

func (qi *QMgrInfo) processHealthWorkerResponse() {
	if qi.qState != QSTATE_buffer {
		level.Error(qi.l).Log("msg", fmt.Sprintf("invalid response from health worker in state %s", qi.qState))
		return
	}
	if len(qi.bids) > 0 {
		level.Info(qi.l).Log("msg", fmt.Sprintf("STATE change to %s from %s", QSTATE_replay, qi.qState))
		qi.qState = QSTATE_replay
		qi.doReplay = true
	} else {
		qi.qState = QSTATE_normal
	}
}

func (qi *QMgrInfo) processBufferReplayResponse(ids []uint64) {
	if qi.qState != QSTATE_replay && qi.qState != QSTATE_replay2 {
		level.Error(qi.l).Log("msg", fmt.Sprintf("Invalid state %s while replay response", qi.qState))
		panic("Invalid qstate")
	}

	if len(ids) != 0 {
		level.Warn(qi.l).Log("msg", fmt.Sprintf("STATE change to %s from %s ids %d", QSTATE_buffer, qi.qState, len(qi.bids)))
		qi.bids = append(ids, qi.bids...)
		qi.qState = QSTATE_buffer
		go qi.serverHealthWorker()
	} else {
		if qi.qState == QSTATE_replay {
			if len(qi.bids) != 0 {
				level.Info(qi.l).Log("msg", fmt.Sprintf("STATE change to %s from %s", QSTATE_replay2, qi.qState))
				qi.qState = QSTATE_replay2
				bids := qi.bids
				qi.bids = make([]uint64, 0, QMGR_bids_alloc_len)
				go qi.replayWorker(bids)
			} else {
				level.Info(qi.l).Log("msg", fmt.Sprintf("STATE change to %s from %s", QSTATE_normal, qi.qState))
				qi.qState = QSTATE_normal
			}
		} else if qi.qState == QSTATE_replay2 {
			if len(qi.bids) != 0 {
				panic(fmt.Sprintf("Found %d buffered ids in replay2 state", len(qi.bids)))
			}
			level.Info(qi.l).Log("msg", fmt.Sprintf("STATE change to %s from %s", QSTATE_normal, qi.qState))
			qi.qState = QSTATE_normal
		}
	}
}

// Main state machine thread.
func (qi *QMgrInfo) mgrThread() {
	level.Info(qi.l).Log("msg", "Zebrium QManager started")
	qi.mgrCtxt, qi.cancelFnc = context.WithCancel(context.Background())

	for {
		waitSecs := qi.nextWakeUpTime.Sub(time.Now())
		if qi.nextWakeUpTime.Before(time.Now()) {
			waitSecs = 0
		}
		select {
		case req := <-qi.reqCh:
			if !qi.stopping {
				qi.processRequest(req)
			}
			qi.mgrStateMachine()
		case reqs := <-qi.reqsCh:
			if !qi.stopping {
				qi.processRequests(reqs)
			}
			qi.mgrStateMachine()
		case _ = <-qi.flushWaitQCh:
			qi.moveAllToRunQ()
			qi.mgrStateMachine()
		case bresp := <-qi.bufferReplayRespCh:
			qi.processBufferReplayResponse(bresp)
			qi.mgrStateMachine()
		case resp := <-qi.bufferWriteRespCh:
			qi.processBufferWriterResponse(resp)
			qi.mgrStateMachine()
		case resp := <-qi.webSenderRespCh:
			qi.processWebSenderResponse(resp)
			qi.mgrStateMachine()
		case <-qi.healthWorkerRespCh:
			qi.processHealthWorkerResponse()
			qi.mgrStateMachine()
		case <-time.After(waitSecs):
			qi.moveAllToRunQ()
			qi.setNextWakeUpTime()
			qi.mgrStateMachine()
		case _ = <-qi.exitMgrCh:
			qi.doStopQMgr()
			return
		}
	}
}

func (qi *QMgrInfo) shouldBufferForError(httpStatusCode int) bool {
	ecodes := []int{500, 503, 504}

	for i := 0; i < len(ecodes); i++ {
		if httpStatusCode == ecodes[i] {
			return true
		}
	}
	return false
}

func (qi *QMgrInfo) sendWebReq(req *ztypes.StWbReq) (*ztypes.StWbResp, bool, error) {
	// This should serialize to gob encoding, compress and send over http.
	// This should take care of retries.

	ctx, cancel := context.WithCancel(qi.mgrCtxt)
	defer cancel()

	timeout := time.Duration(QMGR_http_timeout_secs * time.Second)
	client := http.Client{
		Timeout: timeout,
	}
	if qi.ztr != nil {
		client.Transport = qi.ztr
	}

	pr, pw := io.Pipe()
	gw, err := gzip.NewWriterLevel(pw, gzip.BestSpeed)
	if err != nil {
		level.Warn(qi.l).Log("msg", fmt.Sprintf("gzip new writer failed err %s", err))
		pw.CloseWithError(err)
		pr.CloseWithError(err)
		return nil, false, err
	}

	go func() {
		enc := gob.NewEncoder(gw)
		err := enc.Encode(*req)
		if err != nil {
			level.Warn(qi.l).Log("msg", fmt.Sprintf("Gob encoding failed err %s", err))
			gw.Close()
			pw.CloseWithError(err)
			return
		}
		gw.Flush()
		gw.Close()
		pw.Close()
	}()

	httpReq, err := http.NewRequestWithContext(ctx, "POST", qi.zurl, pr)
	if err != nil {
		level.Warn(qi.l).Log("msg", fmt.Sprintf("Error getting http request err %s", err))
		pr.CloseWithError(err)
		return nil, false, err
	}
	httpReq.Header.Set("Content-type", "application/octet-stream")
	httpReq.Header.Set("Zebrium-Token", qi.ztkn)

	httpRes, err := client.Do(httpReq)
	if err != nil {
		level.Warn(qi.l).Log("msg", fmt.Sprintf("Error sending http request err %s", err))
		pr.CloseWithError(err)
		return nil, true, err
	}
	pr.Close()
	defer httpRes.Body.Close()

	if httpRes.StatusCode != 200 {
		err = fmt.Errorf("HTTP error code %d from server", httpRes.StatusCode)
		level.Warn(qi.l).Log("msg", err.Error())
		return nil, qi.shouldBufferForError(httpRes.StatusCode), err
	}

	dec := gob.NewDecoder(httpRes.Body)
	var res ztypes.StWbResp
	err = dec.Decode(&res)
	if err != nil {
		level.Warn(qi.l).Log("msg", fmt.Sprintf("Invalid response, decode failed, err %s", err))
		return nil, false, err
	}

	return &res, false, nil
}

func (qi *QMgrInfo) isServerOk() bool {
	webReq := &ztypes.StWbReq{Version: 1, Token: qi.ztkn, NInsts: 0, IData: make([]*ztypes.StWbIReq, 0)}
	webRes, _, err := qi.sendWebReq(webReq)
	if err != nil || len(webRes.Err) != 0 {
		return false
	}
	return true
}

// Health worker thread, that starts, once we are in the buffer state.
// This periodically checks to see, if the remote server has come back online.
func (qi *QMgrInfo) serverHealthWorker() {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()

	level.Info(qi.l).Log("msg", fmt.Sprintf("Starting serverHealthCheckWorker"))
	ticks := 0
	alldone := false
	for !alldone {
		select {
		case <-t.C:
			ticks++
			if ticks%30 == 0 && qi.isServerOk() {
				alldone = true
			}
		}
		if qi.stopping {
			alldone = true
		}
	}

	level.Info(qi.l).Log("msg", fmt.Sprintf("Done with serverHealthCheckWorker"))
	qi.healthWorkerRespCh <- true
}

// HTTP worker request info. One instance of this gets created for each HTTP request work.
type workerReq struct {
	reqs      []*ztypes.ZBlobTuple // requests.
	uid       uint64               // unique id.
	qi        *QMgrInfo            // qmgr ingo.
	err       error                // Error, if there is one.
	shdBuffer bool                 // Should we buffer on disk and retry later.
}

func dumpZBlob(l log.Logger, blob *ztypes.StWbIReq) {
	level.Info(l).Log("msg", fmt.Sprintf("Incremental=%v Instance=%s NSamples=%d Gen=%d Samples=%d", blob.IsIncr, blob.Instance, blob.NSamples, blob.Gen, len(blob.Samples)))
}

func dumpZBlobTuple(l log.Logger, r *ztypes.ZBlobTuple) {
	if r.FlBlob != nil {
		dumpZBlob(l, r.FlBlob)
	}
	if r.IncrBlob != nil {
		dumpZBlob(l, r.IncrBlob)
	}
}

// Sends HTTP request,
func (w *workerReq) webSendReqs() int {
	for i := 0; i < len(w.reqs); i++ {
		r := w.reqs[i]
		dumpZBlobTuple(w.qi.l, r)
	}

	var retErr error
	shdBuffer := false
	minPollSecs := 0
	nrLeft := int32(len(w.reqs))

	res2reqmap := make([]int, len(w.reqs))
	for tries := 0; tries < 2; tries++ {

		webReq := &ztypes.StWbReq{Version: 1, Token: w.qi.ztkn, NInsts: nrLeft, IData: make([]*ztypes.StWbIReq, nrLeft)}
		nrAdded := int32(0)
		for i := 0; i < len(w.reqs); i++ {
			if w.reqs[i].FlBlob == nil {
				continue // Request sent already.
			}

			if nrAdded >= nrLeft {
				level.Warn(w.qi.l).Log("msg", fmt.Sprintf("Error incorrect nrLeft %d nrAdded %d", nrLeft, nrAdded))
				retErr = fmt.Errorf("Error incorrect nrLeft %d nrAdded %d", nrLeft, nrAdded)
				break
			}

			if tries == 0 && w.reqs[i].IncrBlob != nil {
				webReq.IData[nrAdded] = w.reqs[i].IncrBlob
			} else {
				webReq.IData[nrAdded] = w.reqs[i].FlBlob
			}
			res2reqmap[nrAdded] = i
			nrAdded++
		}

		if retErr == nil && nrAdded != nrLeft {
			level.Warn(w.qi.l).Log("msg", fmt.Sprintf("Error nrLeft %d not same as nrAdded %d", nrLeft, nrAdded))
			retErr = fmt.Errorf("Error nrLeft %d is not same as nrAdded %d", nrLeft, nrAdded)
		}

		if retErr != nil {
			break
		}

		var webRes *ztypes.StWbResp
		var err error
		webRes, shdBuffer, err = w.qi.sendWebReq(webReq)
		if err != nil || len(webRes.Err) != 0 {
			if err == nil {
				err = fmt.Errorf("Error response from server %s", webRes.Err)
			}
			retErr = err
			break
		}

		if webRes.Version != webReq.Version || webRes.NInsts != webReq.NInsts {
			retErr = fmt.Errorf("Invalid response from server versions %d %d ninsts %d %d", webRes.Version, webReq.Version, webRes.NInsts, webReq.NInsts)
			level.Warn(w.qi.l).Log("msg", retErr.Error())
			break
		}
		minPollSecs = int(webRes.MinPollSecs)

		nrLeft = 0
		nrFailed := 0
		for i := int32(0); i < webRes.NInsts; i++ {
			if webRes.IErrCodes[i] == 0 {
				w.reqs[res2reqmap[i]].FlBlob = nil
			}
			if tries == 0 && webRes.IErrCodes[i] == 1 {
				nrLeft++
			} else if webRes.IErrCodes[i] != 0 {
				nrFailed++
			}
		}

		if nrFailed > 0 {
			retErr = fmt.Errorf("Failed response from server ninsts_failed %d ntotal %d", nrFailed, webRes.NInsts)
			level.Warn(w.qi.l).Log("msg", retErr)
			break
		}

		if nrLeft == 0 {
			break
		}

	}

	w.shdBuffer = shdBuffer
	w.err = retErr
	return minPollSecs
}

// Thread that does the work of sending an HTTP request.
func (w *workerReq) webSenderThread() {

	minPollSecs := w.webSendReqs()

	if !w.shdBuffer {
		// Send the response to mgr thread first, if the completion does not reuse this workerreq..
		w.qi.webSenderRespCh <- w
	}

	// Issue callbacks.
	for i := 0; i < len(w.reqs); i++ {
		w.reqs[i].Cb.SendCompleted(w.err, minPollSecs)
	}

	if w.shdBuffer {
		w.qi.webSenderRespCh <- w
	}
}

func getbufFilePath(bufDir string, id uint64) string {
	return filepath.Join(bufDir, strconv.FormatUint(id, 10))
}

func (w *workerReq) bufferWriteWorkerDone(estr string, cbs []ztypes.ZQueueCb, docb bool) {
	for i := 0; i < len(w.reqs); i++ {
		w.reqs[i].Cb = cbs[i]
	}

	if len(estr) == 0 {
		w.err = nil
	} else {
		level.Warn(w.qi.l).Log("msg", estr)
		w.err = fmt.Errorf("%s", estr)
		w.qi.cleanUpId(w.uid)
	}
	w.qi.bufferWriteRespCh <- w

	// Issue callbacks.
	if docb {
		for i := 0; i < len(w.reqs); i++ {
			w.reqs[i].Cb.SendCompleted(w.err, -1)
		}
	}
}

// Worker that flushes the blobls to disk.
func (w *workerReq) bufferWriteWorker(drop bool, docb bool) {
	ocbs := make([]ztypes.ZQueueCb, len(w.reqs))
	for i := 0; i < len(w.reqs); i++ {
		ocbs[i] = w.reqs[i].Cb
		w.reqs[i].Cb = nil
	}

	if drop {
		w.bufferWriteWorkerDone("", ocbs, docb)
		return
	}

	qi := w.qi

	fpath := getbufFilePath(qi.bufDir, w.uid)
	fh, err := os.Create(fpath)
	if err != nil {
		w.bufferWriteWorkerDone(fmt.Sprintf("Failed to create file %s, err %s", fpath, err), ocbs, docb)
		return
	}
	defer fh.Close()

	gw, err := gzip.NewWriterLevel(fh, gzip.DefaultCompression)
	if err != nil {
		w.bufferWriteWorkerDone(fmt.Sprintf("Failed to create gzip stream for file %s, err %s", fpath, err), ocbs, docb)
		return
	}
	defer gw.Close()

	enc := gob.NewEncoder(gw)
	err = enc.Encode(w.reqs)
	if err != nil {
		w.bufferWriteWorkerDone(fmt.Sprintf("Failed to gob encode %d reqs to file %s, err %s", len(w.reqs), fpath, err), ocbs, docb)
		return
	}

	gw.Flush()
	w.bufferWriteWorkerDone("", ocbs, docb)
	return
}

func (qi *QMgrInfo) cleanUpId(id uint64) {
	fpath := getbufFilePath(qi.bufDir, id)
	os.Remove(fpath) // ignore errors.
}

// Replay worker, that sends the on disk blobs to remote server.
func (qi *QMgrInfo) replayWorker(ids []uint64) {
	level.Info(qi.l).Log("msg", fmt.Sprintf("Starting replay of %d webreqs", len(ids)))
	nrSent := 0
	for i := 0; i < len(ids) && !qi.stopping; i++ {
		id := ids[i]

		fpath := getbufFilePath(qi.bufDir, id)
		if st, err := os.Stat(fpath); err != nil || st.IsDir() {
			continue
		}

		fh, err := os.Open(fpath)
		if err != nil {
			level.Warn(qi.l).Log("msg", fmt.Sprintf("Cannot open file %s, err %s, skipping replay", fpath, err))
			qi.cleanUpId(id)
			continue
		}

		gr, err := gzip.NewReader(fh)
		if err != nil {
			level.Warn(qi.l).Log("msg", fmt.Sprintf("Cannot create gzip stream for file %s, err %s", fpath, err))
			qi.cleanUpId(id)
			continue
		}

		dec := gob.NewDecoder(gr)
		var reqs []*ztypes.ZBlobTuple
		err = dec.Decode(&reqs)
		if err != nil {
			level.Warn(qi.l).Log("msg", fmt.Sprintf("Cannot decode from file %s, err %s, skipping replay", fpath, err))
			qi.cleanUpId(id)
			continue
		}

		wreq := &workerReq{reqs: reqs, uid: id, qi: qi, err: nil, shdBuffer: false}
		wreq.webSendReqs()
		if wreq.shdBuffer {
			level.Warn(qi.l).Log("msg", fmt.Sprintf("Aborting replay to buffer, nrsent %d", i))
			qi.bufferReplayRespCh <- ids[i:]
			return
		}
		if wreq.err == nil {
			nrSent++
		}
		qi.cleanUpId(id)
	}
	level.Info(qi.l).Log("msg", fmt.Sprintf("Done with replay of %d webreqs, nrsent %d", len(ids), nrSent))
	qi.bufferReplayRespCh <- make([]uint64, 0)
}
