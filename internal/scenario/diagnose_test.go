package scenario

import (
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

func TestDiagnoseEmitsOKForNonTriggers(t *testing.T) {
	app := model.NewApplication()
	findings, sum := Diagnose(app)
	if len(findings) != 5 {
		t.Fatalf("findings=%d want 5", len(findings))
	}
	if sum.OK != 5 {
		t.Errorf("ok=%d want 5", sum.OK)
	}
}
