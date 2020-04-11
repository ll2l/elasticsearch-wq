package api

import (
	"github.com/gin-gonic/gin"
	"github.com/ll2l/elasticsearch-wq/ui"
	"html/template"
	"io/ioutil"
	"path"
)

func SetupRoutes(r *gin.Engine) {
	t, err := loadTemplate("index.html")
	if err != nil {
		panic(err)
	}
	r.SetHTMLTemplate(t)

	r.GET("/static/*path", GetAsset)
	r.GET("/", IndexApi)

	apiGroup := r.Group("/api")
	apiGroup.GET("/objects", GetObjects)
	apiGroup.POST("/connect", Connect)
	apiGroup.GET("/connection", GetConnectionInfo)
	apiGroup.GET("/bookmarks", GetBookmarks)
	apiGroup.POST("/switchdb", SwitchCluster)
	apiGroup.GET("/info", GetInfo)
	apiGroup.GET("/clusters", GetClusters)
	apiGroup.GET("/databases", GetClusters)
	apiGroup.GET("/indices/:index/info", GetIndexInfo)
	apiGroup.PUT("/indices/:index", ManageIndex)
	apiGroup.GET("/tables/:table/rows", GetIndexRows)
	apiGroup.GET("/query", RunQuery)
	apiGroup.POST("/query", RunQuery)
	apiGroup.GET("/mapping/:index", GetMapping)
	apiGroup.GET("/kibana", GetKibana)
	apiGroup.GET("/export", DataExport)
	apiGroup.POST("/migrate", Migrate)
	apiGroup.GET("/history", GetHistory)
	apiGroup.GET("/dsl", GetDsl)
	apiGroup.GET("/settings/:index", GetSettings)
	apiGroup.GET("/stats/:index", Getstats)
}

func loadTemplate(name string) (*template.Template, error) {
	t := template.New("")

	f, err := ui.Assets.Open(path.Join("/templates", name))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}
	t, err = t.New(name).Parse(string(b))
	if err != nil {
		return nil, err
	}

	return t, nil
}
