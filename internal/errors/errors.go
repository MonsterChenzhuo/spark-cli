// Package errors defines structured CLI errors with exit-code mapping.
package errors

import (
	"encoding/json"
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
	e, ok := err.(*Error)
	if !ok {
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

func WriteJSON(w io.Writer, err error) {
	if err == nil {
		return
	}
	e, ok := err.(*Error)
	if !ok {
		e = &Error{Code: CodeInternal, Message: err.Error()}
	}
	b, _ := json.Marshal(envelope{Error: e})
	_, _ = w.Write(b)
	_, _ = w.Write([]byte("\n"))
}
