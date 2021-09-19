package bittorrent

import "github.com/prometheus/client_golang/prometheus"

var _ prometheus.Collector = (*Session)(nil)
