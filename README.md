# Zstats collector: A modified Prometheus server.

This is a modified version of prometheus server that streams stats from customer's cluster to a remote server that is running in the cloud.

1. Stats are streamed realtime.
2. Uses very little network bandwidth.
3. It does not store stats locally (no tsdb).
3. Every sample scraped locally will reach the remote server. (i.e no dropping of stats because of timestamp ordering issues).
4. Adds extra labels, that can be used to join these collected stats with the logs. Please see our log collector which adds similar labels [here](https://github.com/zebrium/ze-kubernetes-collector)
5. Accepts the standard prometheus config file and customers can just point their existing prometheus config file to this server.


## Architecture overview

There are two main components of this architecture:

1. **Zstats collector**: Which is a [modified prometheus server](https://github.com/zebrium/prometheus).
2. **Zstats remote server**: This is the remote server, which will receive all the stats from Zstats collector. The code base for this is [here](https://github.com/zebrium/prometheus-remote-server).


![](https://github.com/zebrium/ze-images/blob/master/stats_architecture.png)

This github project is about Zstats collector, which is a modified prometheus server. For Zstats remote server see https://github.com/zebrium/prometheus-remote-server.

### Changes to the Prometheus server:
We have kept the scrapper, service discovery and config parser modules and removed all other modules (yes, no local storage).

We have added three new modules called zpacker, zcacher and zqmgr modules that does all the magic of sending metrics efficiently over the network in real time without loosing any information.

#### Architecture of this customized Prometheus server

![](https://github.com/zebrium/ze-images/blob/master/stats_collector_architecture.png)

##### Basic idea
1. As we scrape a target, we want to send that data immediately to the remote side.
2. The scraped data from a target is like a blob of data that has a set of various metrics and their values with a particular timestamp along with the labels and values.
3. We want to send this blob as efficiently as possible over the wire. This blob contains a set of samples but only one sample of each metric from the scraped target. The number of metrics can change from one scrape to the next scrape as it is up to the exporter on what it provides in each scrape.
4. We want to batch as many blobs as possible into a single request to send over the wire without adding too much latency. This reduces the number of requests that we need to handle on our remote server side.
5. On the remote server side, from the blob(s), we will reconstruct the data as if we scraped the target locally.
6. Every sample that is scraped reaches our remote server without any drops due to out-of-order samples.


##### Encoding of blob over the wire:
Each scraped blob contains a set of metrics. Each metric contains: 
1. help string
2. type information
3. metric name
4. metric label/value pairs
5. one time stamp
6. one value of that sample

The basic idea is, if we have to send the blob as is, even after compression, it still represents a lot of data going over the wire. But if we do the diff of the blob with the blob that was scraped last time from this target, that diff will be very small. We call this diff an incremental blob. This incremental blob is computed as follows:
1. Metric's sample value is diff'd from its last sample value. This difference will be very small or zero in most cases and can be encoded efficiently with variable byte encoding.
2. If the diff value is zero, when the sample value does not change, we skip sending this metric completely in our incremental blob.
3. We also do the diff of the time stamp and do variable byte encoding.
4. If the time stamp is the same across all the metrics of a blob, we detect that and send the timestamp once in the incremental blob instead of per sample.
5. Incremental blob does NOT contain the metric name.
6. Incremental blob does NOT contain the metric's help string.
7. Incremental blob does NOT contain any of the labels or any of the values.

One can see tha the incremental blob contains the data of the samples that changed from the last time we scraped and on top of that we do all the above mentioned optimizations to reduce the size. 

Sending one request to the remote server for each blob is quite expensive in terms of the number of HTTP requests. So, we coalesce blobs, either incremental or full blobs, across all the targets and send them in one request. 

If the scraping interval is too small, we also coalesce blobs from the same target in one request. Each blob, either incremental or full, is also compressedbefore sending on the wire.

On the remote server side, given the incremental blob we should be able to reconstruct the original full blob.

We keep the state between remote server and stats collector as independent as possible, something like a NFS file handle approach. The state is divided between stats collector and remote server as follows:
1. It is the responsibility of the metrics collector to detect that some schema has changed between the last blob and the current blob. This can happen, for example, when we see a new sample this time which did not exist last time or the scraped target came online for the first time. When the stats collector detects this it will NEVER send the incremental blob. Instead it will send the full blob with everything.
2. Metrics collector always creates a full blob in addition to the incremental blob if there is one. However, the metrics collector does not send the full blob over the wire if the incremental blob is available to start with. 
3. If the remote server for any reason cannot reconstruct the full blob from the incremental blob, it will ask the metrics collector to send the full blob. This can happen, if the remote server did not find the last blob in its cache either because the remote server process crashed and came back up or it purged the last blob because of memory pressure.

Sending one request to remote server for each blob is quite expensive in terms of the number of HTTP requests. So, we coalesce blobs either incremental or full blobs across all the targets and send them in one request. If the scraping interval is too small, we also coalesce blobs from the same target in one request. Each blob either, incremental or full, is also compressed before sending on the wire.

We have added three modules to the Prometheus server: zpacker, zcacher, zqmgr
1. Scraper module scrapes the targets periodically from each target. Each time it scrapes from one target, it sends that scraped data to zapcker module. 
2. zpacker (Zebrium packer) module is responsible for buffering and creating two blobs (full, incremental) out of the data that scraper sends.
3. zcacher (Zebrium cacher) module is responsible for caching the last blob.
4. zqmgr (Zebrium queue manager) module is responsible for connection management and coalescing of blobs and sending the HTTP requests to the remote server.


## Install

### Prepackaged container

If you want to install the zebrium prepackaged container, that sends the metrics to our cloud software for anomaly detection, Instructions are [here](https://github.com/zebrium/ze-stats) https://github.com/zebrium/ze-stats.


### Building from source

To build Zstats collector from the source code yourself, you need to have a working
Go environment with [version 1.13 or greater installed](https://golang.org/doc/install).
You will also need to have [Node.js](https://nodejs.org/), [go-kit](https://github.com/go-kit/kit) and [Yarn](https://yarnpkg.com/)
installed in order to build the frontend assets.

You can clone the repository yourself and build using the instructions mentioned below, which will compile and generate prometheus binary that can be run from anywhere:

    $ mkdir -p $GOPATH/src/github.com/prometheus
    $ cd $GOPATH/src/github.com/prometheus
    $ git clone https://github.com/zebrium/prometheus.git
    $ cd prometheus
    $ make build
    $ ./prometheus --zebrium.insecure-ssl  --zebrium.server-url="http://127.0.0.1:9905/api/v1/zstats"  --zebrium.zapi-token=0 --zebrium.local-buffer-dir="/tmp/prom/" --config.file=your_config.yml

Prometheus binary takes these new arguments:
* --zebrium.insecure-ssl : If passed, it uses http instead of https.
* --zebrium.server-url : remote server url to send metrics to.
* --zebrium.zapi-token: Token for authentication.
* --zebrium.local-buffer-dir: local directory to use for buffering metrics, when the remote server is unavailable for a short period of time.

Here is the sample command, that streams metrics to zstats remote server running at http://127.0.0.1:9905/api/v1/zstats
```
    $ ./prometheus --zebrium.insecure-ssl  --zebrium.server-url="http://127.0.0.1:9905/api/v1/zstats"  --zebrium.zapi-token=0 --zebrium.local-buffer-dir="/tmp/prom/" --config.file=your_config.yml
```

For running the zstats remote server, please see [here](https://github.com/zebrium/prometheus-remote-server)


## More information

  * Link to zebrium Blog: TODO 

## License

Apache License 2.0, see [LICENSE](https://github.com/prometheus/prometheus/blob/master/LICENSE).

## Contributors
* Anil Nanduri (Zebrium)
* Dara Hazeghi (Zebrium)
* Brady Zuo (Zebrium)
