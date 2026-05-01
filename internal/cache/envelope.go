// Package cache persists parsed *model.Application snapshots so subsequent
// spark-cli invocations on the same EventLog skip the Open+Decode+Aggregate
// pipeline. See docs/superpowers/specs/2026-05-02-application-cache-design.md.
package cache

import "github.com/opay-bigdata/spark-cli/internal/model"

// currentSchemaVersion is bumped manually whenever model field TYPES change or
// fields are renamed. Adding/removing fields is gob-tolerant and does NOT
// require a bump. Mismatched versions cause cache miss + silent rebuild.
const currentSchemaVersion = 1

// cacheEnvelope is the on-disk payload, gob-encoded then zstd-compressed.
type cacheEnvelope struct {
	SchemaVersion int
	CLIVersion    string
	SourceKind    string // "v1" | "v2"
	SourceKey     sourceKey
	App           *model.Application
}

// sourceKey is the fingerprint used to detect source changes. For V1 logs
// PartCount is always 1 and MaxMtime / TotalSize describe the single file.
// For V2, MaxMtime is the maximum across all parts; TotalSize is the sum;
// PartCount is the count of events_N_* parts.
type sourceKey struct {
	URI       string
	MaxMtime  int64
	TotalSize int64
	PartCount int
}
