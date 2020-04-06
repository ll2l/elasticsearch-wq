// +build ignore

package main

import (
	"log"

	"github.com/ll2l/elasticsearch-wq/ui"
	"github.com/shurcooL/vfsgen"
)

func main() {
	err := vfsgen.Generate(ui.Assets, vfsgen.Options{
		PackageName:  "ui",
		BuildTags:    "buildassert",
		VariableName: "Assets",
	})
	if err != nil {
		log.Fatalln(err)
	}
}

