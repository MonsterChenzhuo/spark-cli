package cmd

import (
	"encoding/json"
	"runtime"

	"github.com/spf13/cobra"

	"github.com/opay-bigdata/spark-cli/internal/selfupdate"
)

func newSelfUpdateCmd() *cobra.Command {
	var opts selfupdate.Options
	cmd := &cobra.Command{
		Use:     "self-update",
		Aliases: []string{"update", "upgrade"},
		Short:   "Update the installed spark-cli binary to the latest release",
		RunE: func(cmd *cobra.Command, args []string) error {
			res, err := selfupdate.Update(cmd.Context(), opts)
			if err != nil {
				return err
			}
			enc := json.NewEncoder(cmd.OutOrStdout())
			enc.SetEscapeHTML(false)
			return enc.Encode(selfUpdateResponse{
				Command: "self-update",
				DryRun:  res.DryRun,
				Version: res.Version,
				Asset:   res.Asset,
				Target:  res.Target,
			})
		},
	}
	cmd.Flags().StringVar(&opts.Version, "version", "", "Release tag to install, e.g. v0.1.2 (default: latest)")
	cmd.Flags().StringVar(&opts.InstallDir, "install-dir", "", "Install directory for spark-cli (default: replace current executable)")
	cmd.Flags().StringVar(&opts.Repo, "repo", selfupdate.DefaultRepo, "GitHub repo slug")
	cmd.Flags().BoolVar(&opts.DryRun, "dry-run", false, "Resolve target asset without downloading or replacing the binary")
	cmd.Flags().StringVar(&opts.GOOS, "os", runtime.GOOS, "Release asset OS override for testing")
	cmd.Flags().StringVar(&opts.GOARCH, "arch", runtime.GOARCH, "Release asset architecture override for testing")
	_ = cmd.Flags().MarkHidden("os")
	_ = cmd.Flags().MarkHidden("arch")
	return cmd
}

type selfUpdateResponse struct {
	Command string `json:"command"`
	DryRun  bool   `json:"dry_run"`
	Version string `json:"version,omitempty"`
	Asset   string `json:"asset,omitempty"`
	Target  string `json:"target"`
}
