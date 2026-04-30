package errors

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestNewIsTypedError(t *testing.T) {
	e := New(CodeAppNotFound, "no app", "check log_dirs")
	if e.Code != CodeAppNotFound || e.Message != "no app" || e.Hint != "check log_dirs" {
		t.Fatalf("unexpected: %+v", e)
	}
	if e.Error() == "" {
		t.Fatal("Error() empty")
	}
}

func TestExitCodeMapping(t *testing.T) {
	cases := []struct {
		code Code
		want int
	}{
		{CodeAppNotFound, 2},
		{CodeFlagInvalid, 2},
		{CodeConfigMissing, 2},
		{CodeHDFSUnreachable, 3},
		{CodeLogUnreadable, 3},
		{CodeLogParseFailed, 1},
		{CodeInternal, 1},
	}
	for _, c := range cases {
		if got := ExitCode(New(c.code, "", "")); got != c.want {
			t.Errorf("%s: got %d want %d", c.code, got, c.want)
		}
	}
	if ExitCode(nil) != 0 {
		t.Fatal("nil should be 0")
	}
}

func TestWriteJSONEnvelope(t *testing.T) {
	var buf bytes.Buffer
	WriteJSON(&buf, New(CodeAppNotFound, "no app", "hint"))
	var out struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
			Hint    string `json:"hint"`
		} `json:"error"`
	}
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Error.Code != "APP_NOT_FOUND" || out.Error.Message != "no app" || out.Error.Hint != "hint" {
		t.Fatalf("unexpected: %+v", out)
	}
}
