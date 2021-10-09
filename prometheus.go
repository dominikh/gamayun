package bittorrent

import (
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
		Value:       sess.statistics.numConnectedPeers.Load(),
		Description: promPeersConnectedDesc,
	}

	// bittorrent_uploaded_raw_bytes
	ch <- Counter{
		Value:       sess.statistics.uploadedRaw.Load(),
		Description: promUploadedRawDesc,
	}

	// bittorrent_downloaded_raw_bytes
	ch <- Counter{
		Value:       sess.statistics.downloadedRaw.Load(),
		Description: promDownloadedRawDesc,
	}

	// bittorrent_peers_rejected_total
	ch <- Counter{
		Value:       sess.statistics.numRejectedPeers.peerIncomingCallback.Load(),
		Description: promPeersRejectedDesc,
		Labels:      promPeersRejectedPeerIncomingCallbackLabel,
	}
	ch <- Counter{
		Value:       sess.statistics.numRejectedPeers.peerHandshakeInfoHashCallback.Load(),
		Description: promPeersRejectedDesc,
		Labels:      promPeersRejectedPeerHandshakeInfoHashCallbackLabel,
	}
	ch <- Counter{
		Value:       sess.statistics.numRejectedPeers.sessionLimit.Load(),
		Description: promPeersRejectedDesc,
		Labels:      promPeersRejectedSessionLimitLabel,
	}
	ch <- Counter{
		Value:       sess.statistics.numRejectedPeers.shutdown.Load(),
		Description: promPeersRejectedDesc,
		Labels:      promPeersRejectedShutdownLabel,
	}
	ch <- Counter{
		Value:       sess.statistics.numRejectedPeers.unknownTorrent.Load(),
		Description: promPeersRejectedDesc,
		Labels:      promPeersRejectedUnknownTorrentLabel,
	}
	ch <- Counter{
		Value:       sess.statistics.numRejectedPeers.stoppedTorrent.Load(),
		Description: promPeersRejectedDesc,
		Labels:      promPeersRejectedStoppedTorrentLabel,
	}

	// bittorrent_torrents
	ch <- Gauge{
		Value:       sess.statistics.numTorrents.stopped.Load(),
		Description: promTorrentsDesc,
		Labels:      promTorrentsStoppedLabel,
	}
	ch <- Gauge{
		Value:       sess.statistics.numTorrents.leeching.Load(),
		Description: promTorrentsDesc,
		Labels:      promTorrentsLeechingLabel,
	}
	ch <- Gauge{
		Value:       sess.statistics.numTorrents.seeding.Load(),
		Description: promTorrentsDesc,
		Labels:      promTorrentsSeedingLabel,
	}
	ch <- Gauge{
		Value:       sess.statistics.numTorrents.action.Load(),
		Description: promTorrentsDesc,
		Labels:      promTorrentsActionLabel,
	}
}
