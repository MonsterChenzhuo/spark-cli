package stats

import "testing"

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
