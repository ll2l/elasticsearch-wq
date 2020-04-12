package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/ll2l/esweb/bookmarks"
	"github.com/ll2l/esweb/client"
	"github.com/ll2l/esweb/ui"
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type status string

const (
	statusSuccess status = "success"
	statusError   status = "error"
)

var EsClient *client.Client

// Send successful response back to client
func respondSuccess(c *gin.Context, data interface{}) {
	c.JSON(200, data)
}

// Send an error response back to client
func respondError(c *gin.Context, err interface{}) {
	var message interface{}

	switch v := err.(type) {
	case error:
		message = v.Error()
	case string:
		message = v
	default:
		message = v
	}
	c.AbortWithStatusJSON(500, gin.H{"status": statusError, "error": message})
}

func IndexApi(c *gin.Context) {
	c.HTML(200, "index.html", gin.H{})
	return
}

func GetAsset(c *gin.Context) {
	serveStaticAsset(c.Params.ByName("path"), c)
}

func serveStaticAsset(path string, c *gin.Context) {
	data, err := ui.Assets.Open("/static" + path)
	if err != nil {
		c.String(400, err.Error())
		return
	}
	b, err := ioutil.ReadAll(data)
	if err != nil {
		return
	}
	c.Data(200, assetContentType(path), b)
}

func Connect(c *gin.Context) {
	host := c.PostForm("host")
	user := c.PostForm("user")
	alias := c.PostForm("alias")
	password := c.PostForm("password")

	cl, err := client.NewFromParams(host, alias, user, password)
	if err != nil {
		respondError(c, err)
		return
	}

	if !cl.Alive() {
		respondError(c, fmt.Sprintf("connect %s failed", host))
		return
	}
	EsClient = cl
	GetConnectionInfo(c)
}

func GetConnectionInfo(c *gin.Context) {
	res, err := EsClient.Info()
	if err != nil {
		respondError(c, err)
		return
	}
	respondSuccess(c, res)
}

// SwitchCluster perform database switch for the client connection
func SwitchCluster(c *gin.Context) {
	name := c.Request.URL.Query().Get("cluster")
	if name == "" {
		name = c.Request.FormValue("cluster")
	}
	if name == "" {
		respondError(c, "Cluster name required")
		return
	}

	conf, err := bookmarks.GetClusterConfig(name)
	if err != nil {
		respondError(c, err)
		return
	}

	cl, err := client.NewFromBookmarks(conf)
	if err != nil {
		respondError(c, err)
		return
	}

	if !cl.Alive() {
		respondError(c, fmt.Errorf("cluster %s is not alive", name))
		return
	}

	EsClient = cl
	GetConnectionInfo(c)
}

func GetObjects(c *gin.Context) {
	// app.js buildSchemaSection
	custerName, err := EsClient.ClusterName()
	if err != nil {
		respondError(c, err)
		return
	}

	indices, err := EsClient.Indices()
	if err != nil {
		respondError(c, err)
		return
	}

	resp := map[string]interface{}{
		custerName: map[string]interface{}{
			"indices": indices,
		},
	}

	respondSuccess(c, resp)
}

func GetClusters(c *gin.Context) {
	respondSuccess(c, bookmarks.GetBookmarks())
}

func GetKibana(c *gin.Context) {
	a := c.Request.FormValue("alias")
	kibanaUrl := bookmarks.GetKibanaUrlByAlias(a)
	respondSuccess(c, gin.H{"kibana": kibanaUrl})
}

func GetBookmarks(c *gin.Context) {
	respondSuccess(c, bookmarks.Clusters)
}

func GetIndexInfo(c *gin.Context) {
	indexName := c.Params.ByName("index")
	res, err := EsClient.IndexInfo(indexName)
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(200, res)
}

func ManageIndex(c *gin.Context) {
	index := c.Params.ByName("index")
	action := c.PostForm("action")

	err := EsClient.ManageIndex(index, action)
	if err != nil {
		respondError(c, err)
		return
	}

	actionStr := action + "ed"
	if strings.HasSuffix(action, "e") {
		actionStr = action + "d"

	}
	c.JSON(200, gin.H{"message": fmt.Sprintf("Successfully %s: [%s]", actionStr, index)})
}

func GetSettings(c *gin.Context) {
	indexName := c.Params.ByName("index")
	res, err := EsClient.Settings(indexName)
	if err != nil {
		respondError(c, err)
		return
	}
	respondSuccess(c, res)
}

func GetStats(c *gin.Context) {
	indexName := c.Params.ByName("index")
	res, err := EsClient.Stats(indexName)
	if err != nil {
		respondError(c, err)
		return
	}
	resp := make(map[string]interface{})
	resp["_shards"] = res["_shards"]
	resp["stats"] = res["indices"].(map[string]interface{})[indexName]
	respondSuccess(c, resp)
}

func GetTasks(c *gin.Context) {
	res, err := EsClient.Tasks()
	if err != nil {
		respondError(c, err)
		return
	}

	var taskTable client.Table
	taskTable.Columns = []string{
		"node", "id", "action", "type", "cancellable", "parent_task_id",
		"running_time_in_nanos", "start_time"}

	for _, v := range res["nodes"].(map[string]interface{}) {
		for _, task := range v.(map[string]interface{})["tasks"].(map[string]interface{}) {
			var row client.Row
			for _, c := range taskTable.Columns {
				if c == "start_time" {
					row = append(row, time.Unix(int64(task.(map[string]interface{})["start_time_in_millis"].(float64)/1000), 0))
				} else {
					row = append(row, task.(map[string]interface{})[c])
				}
			}
			taskTable.Rows = append(taskTable.Rows, row)
		}
	}
	respondSuccess(c, taskTable)
}

func GetMapping(c *gin.Context) {
	indexName := c.Params.ByName("index")
	res, err := EsClient.Mapping(indexName)
	if err != nil {
		respondError(c, err)
		return
	}

	result, ok := res[indexName]
	if !ok {
		respondError(c, fmt.Errorf("%s mapping not found", indexName))
		return
	}

	if c.Request.FormValue("type") == "table" {
		var resultTable client.Table
		resultTable.Columns = []string{"column_name", "data_type"}
		resultTable.Rows = []client.Row{}
		// app.js -> buildTableFilters
		for _, properties := range res[indexName].(map[string]interface{})["mappings"].(map[string]interface{}) {
			for k, v := range properties.(map[string]interface{})["properties"].(map[string]interface{}) {
				ft, ok := v.(map[string]interface{})["type"]
				if !ok {
					ft = "object"
				} else {
					ft = ft.(string)
				}
				resultTable.Rows = append(resultTable.Rows, client.Row{k, ft})
			}
		}
		result = resultTable
	}
	c.JSON(200, result)
}

func GetInfo(c *gin.Context) {
	res, err := EsClient.Info()
	if err != nil {
		respondError(c, err)
		return
	}
	respondSuccess(c, res)
}

func GetIndexRows(c *gin.Context) {
	index := c.Params.ByName("table")
	limit := 100
	offset := 0
	if i, err := strconv.Atoi(c.Query("limit")); err == nil {
		limit = i
	}
	if i, err := strconv.Atoi(c.Query("offset")); err == nil {
		offset = i
	}

	opts := client.RowsOptions{
		Limit:      limit,
		Offset:     offset,
		SortColumn: c.Request.FormValue("sort_column"),
		SortOrder:  c.Request.FormValue("sort_order"),
		Where:      c.Request.FormValue("where"),
	}

	res, err := EsClient.QueryRows(index, opts)
	if err != nil {
		respondError(c, err)
		return
	}

	if res.IsEmpty() {
		respondSuccess(c, gin.H{
			"columns": []string{},
			"rows":    []string{},
		})
		return
	}

	table := res.AsTableRows()

	numFetch := int64(opts.Limit)
	numOffset := int64(opts.Offset)
	numRows := int64(res.Hits.Total)
	numPages := numRows / numFetch

	if numPages*numFetch < numRows {
		numPages++
	}

	table.Pagination = &client.Pagination{
		Rows:    numRows,
		Page:    (numOffset / numFetch) + 1,
		Pages:   numPages,
		PerPage: numFetch,
	}

	respondSuccess(c, table)
}

func RunQuery(c *gin.Context) {
	query := cleanQuery(c.Request.FormValue("query"))
	index := c.Request.FormValue("index")
	editor := c.Request.FormValue("editor")

	if query == "" {
		respondError(c, "Query required")
		return
	}

	if editor == "json" {
		res, err := EsClient.SearchWithBody(index, query)
		if err != nil {
			respondError(c, err)
			return
		}
		respondSuccess(c, res)
	} else {
		HandleQuery(query, c)
	}
}

func HandleQuery(query string, c *gin.Context) {
	rawQuery, err := base64.StdEncoding.DecodeString(desanitize64(query))
	if err == nil {
		query = string(rawQuery)
	}

	res, err := EsClient.Query(query)
	if err != nil {
		badRequest(c, err)
		return
	}

	format := getQueryParam(c, "format")
	filename := getQueryParam(c, "filename")

	if filename == "" {
		filename = fmt.Sprintf("pgweb-%v.%v", time.Now().Unix(), format)
	}

	if format != "" {
		c.Writer.Header().Set("Content-disposition", "attachment;filename="+filename)
	}

	if res.IsEmpty() {
		respondSuccess(c, gin.H{
			"columns": []string{},
			"rows":    []string{},
		})
		return

	}

	result := res.AsTableRows()
	switch format {
	case "csv":
		c.Data(200, "text/csv", result.CSV(true))
	case "json":
		c.Data(200, "application/json", result.JSON())
	case "xml":
		c.XML(200, result)
	default:
		c.JSON(200, result)
	}
}

func DataExport(c *gin.Context) {
	index := strings.TrimSpace(c.Request.FormValue("table"))
	dump := client.Dump{
		Index: index,
	}

	reg := regexp.MustCompile("[^._\\w]+")
	cleanFilename := reg.ReplaceAllString(index, "")

	c.Header(
		"Content-Disposition",
		fmt.Sprintf(`attachment; filename="%s.csv"`, cleanFilename),
	)

	err := dump.Export(EsClient, c.Writer)
	if err != nil {
		badRequest(c, err)
	}
}

func Migrate(c *gin.Context) {
	srcIndex := strings.TrimSpace(c.Request.FormValue("src_index"))
	dstHost := strings.TrimSpace(c.Request.FormValue("dst_host"))
	dstIndex := strings.TrimSpace(c.Request.FormValue("dst_index"))
	dstUser := c.Request.FormValue("dst_user")
	dstPassword := c.Request.FormValue("dst_pass")
	numItems := c.Request.FormValue("num_items")

	if srcIndex == "" || dstIndex == "" {
		respondError(c, "the index cannot be empty")
		return
	}

	if dstHost == "" {
		respondError(c, "destination host cannot be empty")
		return
	}

	numItemsInt, err := strconv.Atoi(numItems)
	if err != nil {
		numItemsInt = 100
	}

	dump := client.Dump{
		Index: srcIndex,
	}

	err = dump.Migrate(EsClient, dstHost, dstUser, dstPassword, dstIndex, numItemsInt)
	if err != nil {
		badRequest(c, err)
		return
	}
	respondSuccess(c, gin.H{"status": "success"})
}

func GetHistory(c *gin.Context) {
	respondSuccess(c, EsClient.History)
}

func GetDsl(c *gin.Context) {
	query := cleanQuery(c.Request.FormValue("query"))
	dsl, _, err := EsClient.GetDsl(query)
	if err != nil {
		respondError(c, err)
		return
	}

	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(dsl), &raw); err != nil {
		respondError(c, fmt.Sprintf("unmarshal failed: %s", dsl))
	}

	respondSuccess(c, raw)
}
