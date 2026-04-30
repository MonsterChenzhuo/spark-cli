package errors

import (
	"bytes"
	"encoding/json"
	"fmt"
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

func TestExitCodeNonTypedError(t *testing.T) {
	if got := ExitCode(fmt.Errorf("plain error")); got != ExitInternal {
		t.Fatalf("plain error: got %d want %d", got, ExitInternal)
	}
}

func TestExitCodeWrappedError(t *testing.T) {
	wrapped := fmt.Errorf("scenario layer: %w", New(CodeAppNotFound, "no app", ""))
	if got := ExitCode(wrapped); got != ExitUserErr {
		t.Fatalf("wrapped APP_NOT_FOUND: got %d want %d", got, ExitUserErr)
	}
	wrapped2 := fmt.Errorf("io layer: %w", New(CodeHDFSUnreachable, "down", ""))
	if got := ExitCode(wrapped2); got != ExitIOErr {
		t.Fatalf("wrapped HDFS_UNREACHABLE: got %d want %d", got, ExitIOErr)
	}
}

func TestExitCodeTypedNil(t *testing.T) {
	var e *Error
	if got := ExitCode(e); got != ExitInternal {
		t.Fatalf("typed-nil: got %d want %d", got, ExitInternal)
	}
}

func TestWriteJSONNilNoOutput(t *testing.T) {
	var buf bytes.Buffer
	WriteJSON(&buf, nil)
	if buf.Len() != 0 {
		t.Fatalf("expected no output, got %q", buf.String())
	}
}

func TestWriteJSONWrappedError(t *testing.T) {
	var buf bytes.Buffer
	wrapped := fmt.Errorf("ctx: %w", New(CodeAppAmbiguous, "two apps", "narrow filter"))
	WriteJSON(&buf, wrapped)
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
	if out.Error.Code != "APP_AMBIGUOUS" || out.Error.Message != "two apps" || out.Error.Hint != "narrow filter" {
		t.Fatalf("unexpected: %+v", out)
	}
}

func TestWriteJSONPlainError(t *testing.T) {
	var buf bytes.Buffer
	WriteJSON(&buf, fmt.Errorf("boom"))
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
	if out.Error.Code != "INTERNAL" || out.Error.Message != "boom" {
		t.Fatalf("unexpected: %+v", out)
	}
}

func TestHintOmitemptyWhenEmpty(t *testing.T) {
	var buf bytes.Buffer
	WriteJSON(&buf, New(CodeInternal, "msg", ""))
	if bytes.Contains(buf.Bytes(), []byte("hint")) {
		t.Fatalf("hint field should be omitted when empty: %s", buf.String())
	}
}
