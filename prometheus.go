package bittorrent

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
)

type Gauge struct {
	Value       uint64
	Description *prometheus.Desc
	Labels      []*dto.LabelPair
}

func (gauge Gauge) Desc() *prometheus.Desc { return gauge.Description }

func (gauge Gauge) Write(m *dto.Metric) error {
	m.Label = gauge.Labels
	m.Gauge = &dto.Gauge{Value: proto.Float64(float64(gauge.Value))}
	return nil
}

type Counter struct {
	Value       uint64
	Description *prometheus.Desc
	Labels      []*dto.LabelPair
}

func (counter Counter) Desc() *prometheus.Desc { return counter.Description }

func (counter Counter) Write(m *dto.Metric) error {
	m.Label = counter.Labels
	m.Counter = &dto.Counter{Value: proto.Float64(float64(counter.Value))}
	return nil
}

func (sess *Session) Describe(chan<- *prometheus.Desc) {
	// Sending no descriptor at all marks the Collector as “unchecked”,
	// i.e. no checks will be performed at registration time, and the
	// Collector may yield any Metric it sees fit in its Collect method.
}

var (
	promPeersConnectedDesc = prometheus.NewDesc("bittorrent_peers_connected", "", nil, nil)
	promUploadedRawDesc    = prometheus.NewDesc("bittorrent_uploaded_raw_bytes", "", nil, nil)
	promDownloadedRawDesc  = prometheus.NewDesc("bittorrent_downloaded_raw_bytes", "", nil, nil)
	promPeersRejectedDesc  = prometheus.NewDesc(
		"bittorrent_peers_rejected_total",
		"The total number of incoming peer connections that have been rejected",
		[]string{"reason"},
		nil,
	)
	promTorrentsDesc = prometheus.NewDesc(
		"bittorrent_torrents",
		"The number of registered torrents, grouped by status",
		[]string{"status"},
		nil,
	)
)

var (
	promPeersRejectedSessionLimitLabel                  = prometheus.MakeLabelPairs(promPeersRejectedDesc, []string{"session_limit"})
	promPeersRejectedPeerIncomingCallbackLabel          = prometheus.MakeLabelPairs(promPeersRejectedDesc, []string{"peer_incoming_callback"})
	promPeersRejectedShutdownLabel                      = prometheus.MakeLabelPairs(promPeersRejectedDesc, []string{"shutdown"})
	promPeersRejectedPeerHandshakeInfoHashCallbackLabel = prometheus.MakeLabelPairs(promPeersRejectedDesc, []string{"peer_handshake_info_hash_callback"})
	promPeersRejectedUnknownTorrentLabel                = prometheus.MakeLabelPairs(promPeersRejectedDesc, []string{"unknown_torrent"})
	promPeersRejectedStoppedTorrentLabel                = prometheus.MakeLabelPairs(promPeersRejectedDesc, []string{"stopped_torrent"})

	promTorrentsStoppedLabel  = prometheus.MakeLabelPairs(promTorrentsDesc, []string{"stopped"})
	promTorrentsLeechingLabel = prometheus.MakeLabelPairs(promTorrentsDesc, []string{"leeching"})
	promTorrentsSeedingLabel  = prometheus.MakeLabelPairs(promTorrentsDesc, []string{"seeding"})
	promTorrentsActionLabel   = prometheus.MakeLabelPairs(promTorrentsDesc, []string{"action"})
)

func (sess *Session) Collect(ch chan<- prometheus.Metric) {
	// bittorrent_peers_connected
	ch <- Gauge{
		Value:       atomic.LoadUint64(&sess.statistics.numConnectedPeers),
		Description: promPeersConnectedDesc,
	}

	// bittorrent_uploaded_raw_bytes
	ch <- Counter{
		Value:       atomic.LoadUint64(&sess.statistics.uploadedRaw),
		Description: promUploadedRawDesc,
	}

	// bittorrent_downloaded_raw_bytes
	ch <- Counter{
		Value:       atomic.LoadUint64(&sess.statistics.downloadedRaw),
		Description: promDownloadedRawDesc,
	}

	// bittorrent_peers_rejected_total
	ch <- Counter{
		Value:       atomic.LoadUint64(&sess.statistics.numRejectedPeers.peerIncomingCallback),
		Description: promPeersRejectedDesc,
		Labels:      promPeersRejectedPeerIncomingCallbackLabel,
	}
	ch <- Counter{
		Value:       atomic.LoadUint64(&sess.statistics.numRejectedPeers.peerHandshakeInfoHashCallback),
		Description: promPeersRejectedDesc,
		Labels:      promPeersRejectedPeerHandshakeInfoHashCallbackLabel,
	}
	ch <- Counter{
		Value:       atomic.LoadUint64(&sess.statistics.numRejectedPeers.sessionLimit),
		Description: promPeersRejectedDesc,
		Labels:      promPeersRejectedSessionLimitLabel,
	}
	ch <- Counter{
		Value:       atomic.LoadUint64(&sess.statistics.numRejectedPeers.shutdown),
		Description: promPeersRejectedDesc,
		Labels:      promPeersRejectedShutdownLabel,
	}
	ch <- Counter{
		Value:       atomic.LoadUint64(&sess.statistics.numRejectedPeers.unknownTorrent),
		Description: promPeersRejectedDesc,
		Labels:      promPeersRejectedUnknownTorrentLabel,
	}
	ch <- Counter{
		Value:       atomic.LoadUint64(&sess.statistics.numRejectedPeers.stoppedTorrent),
		Description: promPeersRejectedDesc,
		Labels:      promPeersRejectedStoppedTorrentLabel,
	}

	// bittorrent_torrents
	ch <- Gauge{
		Value:       atomic.LoadUint64(&sess.statistics.numTorrents.stopped),
		Description: promTorrentsDesc,
		Labels:      promTorrentsStoppedLabel,
	}
	ch <- Gauge{
		Value:       atomic.LoadUint64(&sess.statistics.numTorrents.leeching),
		Description: promTorrentsDesc,
		Labels:      promTorrentsLeechingLabel,
	}
	ch <- Gauge{
		Value:       atomic.LoadUint64(&sess.statistics.numTorrents.seeding),
		Description: promTorrentsDesc,
		Labels:      promTorrentsSeedingLabel,
	}
	ch <- Gauge{
		Value:       atomic.LoadUint64(&sess.statistics.numTorrents.action),
		Description: promTorrentsDesc,
		Labels:      promTorrentsActionLabel,
	}
}
