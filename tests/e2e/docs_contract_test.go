package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDocsDescribeSHSProgressAsJSONEvents(t *testing.T) {
	files := []string{
		"README.md",
		"README.zh.md",
		filepath.Join(".agents", "skills", "spark", "SKILL.md"),
		filepath.Join(".claude", "skills", "spark", "SKILL.md"),
	}
	for _, rel := range files {
		t.Run(rel, func(t *testing.T) {
			body, err := os.ReadFile(filepath.Join("..", "..", rel))
			if err != nil {
				t.Fatal(err)
			}
			text := string(body)
			for _, forbidden := range []string{
				"first-fetch progress is printed to stderr",
				"不打 SHS zip 下载进度提示",
			} {
				if strings.Contains(text, forbidden) {
					t.Fatalf("%s still describes SHS progress as text: %q", rel, forbidden)
				}
			}
			if !strings.Contains(text, "SHS_DOWNLOAD_START") {
				t.Fatalf("%s should mention SHS_DOWNLOAD_START JSON event", rel)
			}
		})
	}
}
