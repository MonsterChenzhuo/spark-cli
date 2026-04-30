package eventlog

import (
	"fmt"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strconv"
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
		if src, ok, err := l.resolveV2(fsys, dir, appID); err != nil {
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

var v2PartRE = regexp.MustCompile(`^events_(\d+)_(.+)$`)

func (l *Locator) resolveV2(fsys fs.FS, dirURI, appID string) (LogSource, bool, error) {
	v2Name := "eventlog_v2_" + appID
	dirs, err := fsys.List(dirURI, v2Name)
	if err != nil {
		return LogSource{}, false, cerrors.New(cerrors.CodeLogUnreadable, err.Error(), "")
	}
	var v2URI string
	for _, d := range dirs {
		if path.Base(d) == v2Name {
			st, err := fsys.Stat(d)
			if err != nil {
				return LogSource{}, false, cerrors.New(cerrors.CodeLogUnreadable, err.Error(), "")
			}
			if st.IsDir {
				v2URI = d
				break
			}
		}
	}
	if v2URI == "" {
		return LogSource{}, false, nil
	}
	parts, err := fsys.List(v2URI, "events_")
	if err != nil {
		return LogSource{}, false, cerrors.New(cerrors.CodeLogUnreadable, err.Error(), "")
	}
	type indexed struct {
		idx         int
		uri         string
		base        string
		compression Compression
	}
	var entries []indexed
	incomplete := false
	for _, p := range parts {
		base := path.Base(p)
		stripped := stripEventLogSuffixes(base)
		m := v2PartRE.FindStringSubmatch(stripped)
		if m == nil {
			continue
		}
		n, _ := strconv.Atoi(m[1])
		entries = append(entries, indexed{
			idx:         n,
			uri:         p,
			base:        base,
			compression: DetectCompression(base),
		})
		if strings.HasSuffix(base, ".inprogress") {
			incomplete = true
		}
	}
	if len(entries) == 0 {
		return LogSource{}, false, nil
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].idx < entries[j].idx })
	for i, it := range entries {
		if it.idx != i+1 {
			return LogSource{}, false, cerrors.New(cerrors.CodeLogIncomplete,
				fmt.Sprintf("V2 EventLog %s missing part %d", appID, i+1),
				"check if upload is complete")
		}
	}
	compression := entries[0].compression
	for _, it := range entries[1:] {
		if it.compression != compression {
			return LogSource{}, false, cerrors.New(cerrors.CodeLogIncomplete,
				fmt.Sprintf("V2 EventLog %s has mixed codecs: part 1 is %s, %s is %s",
					appID, compression, it.base, it.compression),
				"all parts must share a single codec")
		}
	}
	var totalSize int64
	urls := make([]string, 0, len(entries))
	for _, it := range entries {
		urls = append(urls, it.uri)
		if st, err := fsys.Stat(it.uri); err == nil {
			totalSize += st.Size
		}
	}
	return LogSource{
		URI:         v2URI,
		Format:      "v2",
		Compression: compression,
		Incomplete:  incomplete,
		Parts:       urls,
		SizeBytes:   totalSize,
	}, true, nil
}
