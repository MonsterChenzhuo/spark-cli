// Package stats wraps t-digest for percentile estimates.
package stats

import "github.com/influxdata/tdigest"

type Digest struct {
	td    *tdigest.TDigest
	count int
}

func NewDigest() *Digest {
	return &Digest{td: tdigest.NewWithCompression(100)}
}

func (d *Digest) Add(v float64) {
	d.td.Add(v, 1)
	d.count++
}

func (d *Digest) Count() int { return d.count }

func (d *Digest) Quantile(q float64) float64 {
	if d.count == 0 {
		return 0
	}
	return d.td.Quantile(q)
}
