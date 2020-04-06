// +build !buildassert

package ui

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/shurcooL/httpfs/filter"
	"github.com/shurcooL/httpfs/union"
)

// Assets contains the project's assets.
var Assets http.FileSystem = func() http.FileSystem {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	var assetsPrefix string
	switch filepath.Base(wd) {
	case "elasticsearch-wq":
		// When running Prometheus (without built-in assets) from the repo root.
		assetsPrefix = filepath.Join(wd, "/ui")
	case "ui":
		// When generating statically compiled-in assets.
		assetsPrefix = wd
	}

	static := filter.Keep(
		http.Dir(filepath.Join(assetsPrefix, "static")),
		func(path string, fi os.FileInfo) bool {
			return fi.IsDir() ||
				(!strings.HasSuffix(path, "map.js") &&
					!strings.HasSuffix(path, "/bootstrap.js") &&
					!strings.HasSuffix(path, "/bootstrap-theme.css"))
		},
	)

	templates := filter.Keep(
		http.Dir(filepath.Join(assetsPrefix, "templates")),
		func(path string, fi os.FileInfo) bool {
			return fi.IsDir() || strings.HasSuffix(path, ".html")
		},
	)

	return union.New(map[string]http.FileSystem{
		"/templates": templates,
		"/static":    static,
	})
}()

