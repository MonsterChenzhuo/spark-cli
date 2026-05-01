package stats

import (
	"bytes"
	"encoding/gob"
	"math"
	"testing"
)

func TestDigestEmptyReturnsZero(t *testing.T) {
	d := NewDigest()
	if d.Quantile(0.5) != 0 {
		t.Fatal("empty digest must return 0")
	}
}

func TestDigestRoughPercentile(t *testing.T) {
	d := NewDigest()
	for i := 1; i <= 1000; i++ {
		d.Add(float64(i))
	}
	median := d.Quantile(0.5)
	if median < 480 || median > 520 {
		t.Fatalf("median %f out of expected range", median)
	}
	p99 := d.Quantile(0.99)
	if p99 < 985 || p99 > 1000 {
		t.Fatalf("p99 %f out of expected range", p99)
	}
}

func TestDigestGobRoundTrip(t *testing.T) {
	d := NewDigest()
	for i := 1; i <= 1000; i++ {
		d.Add(float64(i))
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(d); err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out *Digest
	if err := gob.NewDecoder(&buf).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Count() != d.Count() {
		t.Fatalf("count: got %d want %d", out.Count(), d.Count())
	}
	for _, q := range []float64{0.5, 0.9, 0.95, 0.99} {
		want := d.Quantile(q)
		got := out.Quantile(q)
		if math.Abs(got-want) > math.Max(want*0.005, 0.5) {
			t.Errorf("quantile %.2f: got %f want %f", q, got, want)
		}
	}
}

func TestDigestGobEmpty(t *testing.T) {
	d := NewDigest()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(d); err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out *Digest
	if err := gob.NewDecoder(&buf).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Count() != 0 {
		t.Fatalf("count: got %d want 0", out.Count())
	}
	if got := out.Quantile(0.5); got != 0 {
		t.Errorf("empty quantile: got %f want 0", got)
	}
}
