// Package errors defines structured CLI errors with exit-code mapping.
package errors

import (
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
)

type Code string

const (
	CodeConfigMissing   Code = "CONFIG_MISSING"
	CodeAppNotFound     Code = "APP_NOT_FOUND"
	CodeAppAmbiguous    Code = "APP_AMBIGUOUS"
	CodeLogUnreadable   Code = "LOG_UNREADABLE"
	CodeLogParseFailed  Code = "LOG_PARSE_FAILED"
	CodeLogIncomplete   Code = "LOG_INCOMPLETE"
	CodeHDFSUnreachable Code = "HDFS_UNREACHABLE"
	CodeFlagInvalid     Code = "FLAG_INVALID"
	CodeInternal        Code = "INTERNAL"
)

const (
	ExitOK       = 0
	ExitInternal = 1
	ExitUserErr  = 2
	ExitIOErr    = 3
)

type Error struct {
	Code    Code   `json:"code"`
	Message string `json:"message"`
	Hint    string `json:"hint,omitempty"`
}

type Event struct {
	Code    string         `json:"code"`
	Level   string         `json:"level"`
	Message string         `json:"message"`
	Hint    string         `json:"hint,omitempty"`
	Fields  map[string]any `json:"fields,omitempty"`
}

func New(code Code, msg, hint string) *Error {
	return &Error{Code: code, Message: msg, Hint: hint}
}

func (e *Error) Error() string {
	if e.Hint != "" {
		return fmt.Sprintf("%s: %s (hint: %s)", e.Code, e.Message, e.Hint)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func ExitCode(err error) int {
	if err == nil {
		return ExitOK
	}
	var e *Error
	if !stderrors.As(err, &e) || e == nil {
		return ExitInternal
	}
	switch e.Code {
	case CodeConfigMissing, CodeAppNotFound, CodeAppAmbiguous, CodeFlagInvalid:
		return ExitUserErr
	case CodeHDFSUnreachable, CodeLogUnreadable, CodeLogIncomplete:
		return ExitIOErr
	default:
		return ExitInternal
	}
}

type envelope struct {
	Error *Error `json:"error"`
}

type eventEnvelope struct {
	Event *Event `json:"event"`
}

func WriteJSON(w io.Writer, err error) {
	if err == nil {
		return
	}
	var e *Error
	if !stderrors.As(err, &e) || e == nil {
		e = &Error{Code: CodeInternal, Message: err.Error()}
	}
	b, _ := json.Marshal(envelope{Error: e})
	_, _ = w.Write(b)
	_, _ = w.Write([]byte("\n"))
}

func WriteEventJSON(w io.Writer, event Event) {
	if w == nil {
		return
	}
	if event.Level == "" {
		event.Level = "info"
	}
	b, _ := json.Marshal(eventEnvelope{Event: &event})
	_, _ = w.Write(b)
	_, _ = w.Write([]byte("\n"))
}
