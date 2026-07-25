// Package web embeds the built dashboard (web/dist, produced by `npm run
// build`) into the aegisd binary so a single compiled server can serve the
// API and the UI with no separate static file deployment step.
package web

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var distFS embed.FS

// DistFS returns the embedded dashboard build with the "dist" prefix
// stripped, so "index.html" and asset paths resolve exactly as the browser
// requests them.
func DistFS() fs.FS {
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		panic(err) // programmer error: dist/ is embedded above, this can only fail if the layout changes
	}
	return sub
}
