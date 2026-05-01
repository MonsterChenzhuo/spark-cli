package scenario

import (
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

func TestDiagnoseEmitsOKForNonTriggers(t *testing.T) {
	app := model.NewApplication()
	findings, sum := Diagnose(app)
	if len(findings) != 6 {
		t.Fatalf("findings=%d want 6", len(findings))
	}
	if sum.OK != 6 {
		t.Errorf("ok=%d want 6", sum.OK)
	}
}
