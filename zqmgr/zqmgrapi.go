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

package zqmgr

import (
	"container/list"
	"time"

	"github.com/go-kit/kit/log"

	"github.com/prometheus/prometheus/ztypes"
)

var gqi *QMgrInfo

// Inits the queue manager.
func InitQMgr(url string, insecureSSL bool, token string, bufDir string, mqd int,
	misToCoelesce int, bufWaitSecs int, maxQueuedScrapesPerInstance int, maxBufs int, l log.Logger) error {
	gqi = GetQMgrInfo(url, insecureSSL, token, bufDir, mqd, misToCoelesce, bufWaitSecs,
		maxQueuedScrapesPerInstance, maxBufs, l)
	return gqi.initQMgr()
}

// Enqueues blobtuple, called by zpacker.
// Tuple: contains incremental and full blob. Well incremental is optional.
func EnqueueBlobTuple(req *ztypes.ZBlobTuple) error {
	gqi.enqueueBlobTuple(req)
	return nil
}

// Enqueues a set of blob tuples.
func EnqueueBlobTuples(reqs *list.List) error {
	gqi.enqueueBlobTuples(reqs)
	return nil
}

// Flush the waitq.
// Queue manager might defer the requests, to coelesce more, this will flush whatever is avaialble now.
func FlushWaitQReq() {
	gqi.flushWaitQReq()
}

// Stop the queue manager.
func StopQMgr() {
	gqi.stopQMgr()
}

func RegisterZPackerInstance(interval time.Duration) int {
	return gqi.addScrapeInterval(int(interval / time.Second))
}

func UnregisterZPackerInstance(secs int) {
	gqi.removeScrapeInterval(secs)
}
