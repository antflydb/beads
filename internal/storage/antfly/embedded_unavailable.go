//go:build !cgo || (!darwin && !linux)

package antfly

import "errors"

func openEmbedded(Target) (Transport, error) {
	return nil, errors.New("embedded Antfly requires a cgo build on macOS or Linux; use a remote Antfly URL with this build")
}
