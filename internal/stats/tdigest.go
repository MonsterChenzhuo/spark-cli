// Package stats wraps t-digest for percentile estimates.
package stats

import (
	"bytes"
	"encoding/gob"

	"github.com/influxdata/tdigest"
)

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

// digestWire is the on-the-wire shape of *Digest. The unexported fields of
// Digest can't be reached by encoding/gob directly; we round-trip through the
// exported tdigest.Centroid list (Mean, Weight).
type digestWire struct {
	Count     int
	Centroids []tdigest.Centroid
}

func (d *Digest) GobEncode() ([]byte, error) {
	w := digestWire{Count: d.count}
	if d.td != nil {
		cs := d.td.Centroids()
		w.Centroids = append(w.Centroids, cs...)
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(w); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (d *Digest) GobDecode(data []byte) error {
	var w digestWire
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&w); err != nil {
		return err
	}
	d.td = tdigest.NewWithCompression(100)
	if len(w.Centroids) > 0 {
		d.td.AddCentroidList(tdigest.NewCentroidList(append([]tdigest.Centroid(nil), w.Centroids...)))
	}
	d.count = w.Count
	return nil
}
