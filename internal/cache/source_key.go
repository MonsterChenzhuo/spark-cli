package cache

import (
	"github.com/opay-bigdata/spark-cli/internal/eventlog"
	"github.com/opay-bigdata/spark-cli/internal/fs"
)

// computeSourceKey stats the underlying source(s) and returns the fingerprint.
// For V1, it stats the single LogSource.URI. For V2, it stats every Part.
// Any Stat error returns ok=false; the caller should treat that as miss.
func computeSourceKey(src eventlog.LogSource, fsys fs.FS) (sourceKey, bool) {
	switch src.Format {
	case "v1":
		st, err := fsys.Stat(src.URI)
		if err != nil {
			return sourceKey{}, false
		}
		return sourceKey{
			URI:       src.URI,
			MaxMtime:  st.ModTime,
			TotalSize: st.Size,
			PartCount: 1,
		}, true
	case "v2":
		var maxMtime, total int64
		for _, p := range src.Parts {
			st, err := fsys.Stat(p)
			if err != nil {
				return sourceKey{}, false
			}
			if st.ModTime > maxMtime {
				maxMtime = st.ModTime
			}
			total += st.Size
		}
		return sourceKey{
			URI:       src.URI,
			MaxMtime:  maxMtime,
			TotalSize: total,
			PartCount: len(src.Parts),
		}, true
	default:
		return sourceKey{}, false
	}
}
