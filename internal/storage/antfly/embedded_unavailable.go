//go:build !cgo || (!darwin && !linux)

package antfly

import (
	"context"
	"errors"
)

func openEmbedded(Target) (Transport, error) {
	return nil, errors.New("embedded Antfly requires a cgo build on macOS or Linux; use a remote Antfly URL with this build")
}

// RestoreDatabaseBackupFile requires the native engine on a supported platform.
func RestoreDatabaseBackupFile(context.Context, string, string, string) error {
	return errors.New("native database restore requires a cgo build on macOS or Linux")
}
