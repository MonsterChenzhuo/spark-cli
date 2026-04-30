package eventlog

import (
	"fmt"
	"net/url"
	"path"
	"strings"

	"github.com/opay-bigdata/spark-cli/internal/fs"

	cerrors "github.com/opay-bigdata/spark-cli/internal/errors"
)

// LogSource describes a resolved EventLog. Format == "v1" means URI points at
// a single file and Parts is nil; Format == "v2" means URI points at the
// rolling-format directory and Parts holds the ordered events_N_* URIs.
type LogSource struct {
	URI         string
	Format      string // "v1" | "v2"
	Compression Compression
	Incomplete  bool
	Parts       []string // V2 ordered URIs; V1 nil
	SizeBytes   int64
}

// stripEventLogSuffixes peels .inprogress (possibly repeated) and a single
// trailing codec extension. Shared between normalizeAppID and resolveV1 so the
// two stay in lockstep when codec extensions are added.
func stripEventLogSuffixes(s string) string {
	for strings.HasSuffix(s, ".inprogress") {
		s = strings.TrimSuffix(s, ".inprogress")
	}
	for _, ext := range []string{".zstd", ".lz4", ".snappy"} {
		if strings.HasSuffix(s, ext) {
			return strings.TrimSuffix(s, ext)
		}
	}
	return s
}

type Locator struct {
	fsByScheme map[string]fs.FS
	logDirs    []string
}

func NewLocator(fsByScheme map[string]fs.FS, logDirs []string) *Locator {
	return &Locator{fsByScheme: fsByScheme, logDirs: logDirs}
}

func normalizeAppID(s string) string {
	s = stripEventLogSuffixes(s)
	if !strings.HasPrefix(s, "application_") && !strings.HasPrefix(s, "eventlog_v2_application_") {
		s = "application_" + s
	}
	return s
}

// Resolve walks logDirs in order and returns the first match. Within a single
// dir, multiple matches surface as APP_AMBIGUOUS; across dirs, the earlier
// dir wins silently — config order is treated as priority.
func (l *Locator) Resolve(appIDInput string) (LogSource, error) {
	appID := normalizeAppID(appIDInput)
	for _, dir := range l.logDirs {
		fsys, err := l.fsFor(dir)
		if err != nil {
			return LogSource{}, err
		}
		if src, ok, err := l.resolveV1(fsys, dir, appID); err != nil {
			return LogSource{}, err
		} else if ok {
			return src, nil
		}
	}
	return LogSource{}, cerrors.New(cerrors.CodeAppNotFound,
		fmt.Sprintf("no EventLog matching %s in any log_dir", appID),
		"check log_dirs in config or pass --log-dirs")
}

func (l *Locator) fsFor(dirURI string) (fs.FS, error) {
	u, err := url.Parse(dirURI)
	if err != nil {
		return nil, cerrors.New(cerrors.CodeFlagInvalid, "bad log_dir: "+dirURI, "use file:// or hdfs:// URI")
	}
	fsys, ok := l.fsByScheme[u.Scheme]
	if !ok {
		return nil, cerrors.New(cerrors.CodeFlagInvalid, "unsupported scheme: "+u.Scheme, "use file:// or hdfs://")
	}
	return fsys, nil
}

func (l *Locator) resolveV1(fsys fs.FS, dirURI, appID string) (LogSource, bool, error) {
	all, err := fsys.List(dirURI, appID)
	if err != nil {
		return LogSource{}, false, cerrors.New(cerrors.CodeLogUnreadable, err.Error(), "")
	}
	var matches []string
	for _, uri := range all {
		if stripEventLogSuffixes(path.Base(uri)) == appID {
			matches = append(matches, uri)
		}
	}
	if len(matches) == 0 {
		return LogSource{}, false, nil
	}
	if len(matches) > 1 {
		return LogSource{}, false, cerrors.New(cerrors.CodeAppAmbiguous,
			fmt.Sprintf("multiple matches for %s: %v", appID, matches),
			"give the full applicationId including timestamp")
	}
	uri := matches[0]
	st, err := fsys.Stat(uri)
	if err != nil {
		return LogSource{}, false, cerrors.New(cerrors.CodeLogUnreadable, err.Error(), "")
	}
	base := path.Base(uri)
	return LogSource{
		URI:         uri,
		Format:      "v1",
		Compression: DetectCompression(base),
		Incomplete:  strings.HasSuffix(base, ".inprogress"),
		SizeBytes:   st.Size,
	}, true, nil
}
