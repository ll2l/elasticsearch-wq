package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v6"
	"github.com/elastic/go-elasticsearch/v6/esapi"
	"github.com/ll2l/elasticsql"
	"github.com/ll2l/esweb/bookmarks"
	"log"
	"regexp"
	"strings"
)

var (
	DefaultCluster  = "http://localhost:9200"
	DefaultUser     = ""
	DefaultPassword = ""
	DefaultAlias    = ""
	History         map[string][]Record
)

type Client struct {
	es            *elasticsearch.Client
	serverVersion string
	clusterName   string
	Alias         string
	KibanaUrl     string
}

func New() (*Client, error) {
	return NewFromParams(DefaultCluster, DefaultAlias, DefaultUser, DefaultPassword)
}

func NewFromParams(host, alias, user, password string) (*Client, error) {
	cfg := elasticsearch.Config{}
	cfg.Addresses = []string{host}

	if user != "" && password != "" {
		cfg.Username = user
		cfg.Password = password
	}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	s := Client{
		es:    client,
		Alias: alias,
	}
	s.SetServerVersion()
	return &s, nil
}

func NewFromBookmarks(conf bookmarks.Bookmark) (*Client, error) {
	cfg := elasticsearch.Config{
		Addresses: conf.Addresses,
	}

	if conf.User != "" && conf.Password != "" {
		cfg.Username = conf.User
		cfg.Password = conf.Password
	}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	s := Client{
		es:    client,
		Alias: conf.Alias,
	}
	s.SetServerVersion()
	return &s, nil
}

func (c *Client) Alive() bool {
	res, err := c.es.Ping()
	if err := checkElasticResp(res, err); err != nil {
		return false
	}
	defer res.Body.Close()
	return true
}

func (c *Client) ClusterName() (string, error) {
	clusterState := c.es.Cluster.State
	res, err := clusterState(
		clusterState.WithMetric("indices"),
		clusterState.WithPretty(),
	)

	if err := checkElasticResp(res, err); err != nil {
		return "", err
	}
	defer res.Body.Close()

	var m map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&m); err != nil {
		return "", err
	}
	return m["cluster_name"].(string), nil
}

func (c *Client) Info() (map[string]interface{}, error) {
	res, err := c.es.Info()

	if err := checkElasticResp(res, err); err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %c", err)
	}
	r["alias"] = c.Alias
	for k, v := range r["version"].(map[string]interface{}) {
		r["version."+k] = v
	}
	delete(r, "version")
	return r, nil
}

func (c *Client) SetServerVersion() {
	i, err := c.Info()
	if err == nil {
		c.serverVersion = i["version.number"].(string)
	}
}

func (c *Client) Indices() ([]interface{}, error) {
	res, err := c.es.Cat.Indices(
		c.es.Cat.Indices.WithFormat("json"),
		c.es.Cat.Indices.WithV(true),
	)

	if err := checkElasticResp(res, err); err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var i []interface{}
	if err := json.NewDecoder(res.Body).Decode(&i); err != nil {
		return nil, err
	}

	return i, nil
}

func (c *Client) Mapping(indexName string) (map[string]interface{}, error) {
	res, err := c.es.Indices.GetMapping(
		c.es.Indices.GetMapping.WithIndex(indexName),
		c.es.Indices.GetMapping.WithPretty(),
		c.es.Indices.GetMapping.WithHuman(),
	)

	if err := checkElasticResp(res, err); err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var m map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *Client) Settings(indexName string) (map[string]interface{}, error) {
	res, err := c.es.Indices.GetSettings(
		c.es.Indices.GetSettings.WithIndex(indexName),
		c.es.Indices.GetSettings.WithPretty(),
		c.es.Indices.GetSettings.WithIncludeDefaults(true),
	)

	if err := checkElasticResp(res, err); err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var m map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *Client) Stats(indexName string) (map[string]interface{}, error) {
	res, err := c.es.Indices.Stats(
		c.es.Indices.Stats.WithIndex(indexName),
		c.es.Indices.Stats.WithPretty(),
	)

	if err := checkElasticResp(res, err); err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var m map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *Client) Tasks() (map[string]interface{}, error) {
	res, err := c.es.Tasks.List()

	if err := checkElasticResp(res, err); err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var m map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&m); err != nil {
		return nil, err
	}

	return m, nil
}

func (c *Client) IndexInfo(indexNames string) (map[string]interface{}, error) {
	res, err := c.es.Cat.Indices(
		c.es.Cat.Indices.WithFormat("json"),
		c.es.Cat.Indices.WithIndex(indexNames),
	)

	if err := checkElasticResp(res, err); err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var i []map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&i); err != nil {
		return nil, err
	}

	return i[0], nil
}

func (c *Client) ManageIndex(index string, action string) error {
	var (
		err error
		res *esapi.Response
	)

	switch action {
	case "delete":
		res, err = c.es.Indices.Delete([]string{index})
	case "refresh":
		res, err = c.es.Indices.Refresh(c.es.Indices.Refresh.WithIndex(index))
	case "flush":
		res, err = c.es.Indices.Flush(c.es.Indices.Flush.WithIndex(index))
	case "merge":
		res, err = c.es.Indices.Forcemerge(c.es.Indices.Forcemerge.WithIndex(index))
	case "clear_cache":
		res, err = c.es.Indices.ClearCache(c.es.Indices.ClearCache.WithIndex(index))
	case "freeze":
		res, err = c.es.Indices.Freeze(index)
	case "close":
		res, err = c.es.Indices.Close([]string{index})
	case "open":
		res, err = c.es.Indices.Open([]string{index})
	}

	if err := checkElasticResp(res, err); err != nil {
		return err
	}
	defer res.Body.Close()

	return nil

}

func (c *Client) QueryRows(indexName string, opts RowsOptions) (*searchResponse, error) {
	var r searchResponse

	// Perform the search request.
	res, err := c.es.Search(
		c.es.Search.WithContext(context.Background()),
		c.es.Search.WithIndex(indexName),
		c.es.Search.WithBody(opts.buildRowsQuery()),
		c.es.Search.WithFrom(opts.Offset),
		c.es.Search.WithSize(opts.Limit),
	)

	if err := checkElasticResp(res, err); err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Printf("Error parsing the response body: %c", err)
		return nil, err
	}
	return &r, nil
}

func (c *Client) Query(sql string) (*searchResponse, error) {
	dsl, index, err := c.GetDsl(sql)

	if err != nil {
		return nil, err
	}

	if !c.hasHistoryRecord(sql) {
		History[c.Alias] = append(History[c.Alias], newHistoryRecord(sql))
	}

	return c.Search(index, dsl)
}

func (c *Client) Search(indexName string, body string) (*searchResponse, error) {
	var r searchResponse

	// Build the request body.
	var buf bytes.Buffer
	buf.WriteString(body)

	// Perform the search request.
	res, err := c.es.Search(
		c.es.Search.WithContext(context.Background()),
		c.es.Search.WithIndex(indexName),
		c.es.Search.WithBody(&buf),
		c.es.Search.WithTrackTotalHits(true),
		c.es.Search.WithPretty(),
	)

	if err := checkElasticResp(res, err); err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Printf("Error parsing the response body: %c", err)
		return nil, err
	}

	return &r, nil
}

func (c *Client) SearchWithBody(index, body string) (map[string]interface{}, error) {
	var buf bytes.Buffer
	buf.WriteString(body)

	// Perform the search request.
	res, err := c.es.Search(
		c.es.Search.WithContext(context.Background()),
		c.es.Search.WithIndex(index),
		c.es.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %c", err)
	}

	if !c.hasHistoryRecord(body) {
		History[c.Alias] = append(History[c.Alias], newHistoryRecord(body))
	}
	return r, nil
}

func (c *Client) Export(indexName string, body string) (*searchResponse, error) {
	var r searchResponse

	// Build the request body.
	var buf bytes.Buffer
	buf.WriteString(body)

	fmt.Println("index name:", indexName)
	fmt.Println(body)

	// Perform the search request.
	res, err := c.es.Search(
		c.es.Search.WithContext(context.Background()),
		c.es.Search.WithIndex(indexName),
		c.es.Search.WithBody(&buf),
		c.es.Search.WithTrackTotalHits(true),
		c.es.Search.WithPretty(),
	)

	if err := checkElasticResp(res, err); err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Printf("Error parsing the response body: %c", err)
		return nil, err
	}
	return &r, nil
}

func (c *Client) hasHistoryRecord(query string) bool {
	result := false

	for _, record := range History[c.Alias] {
		if record.Query == query {
			result = true
			break
		}
	}

	return result
}

func (c *Client) GetDsl(sql string) (dsl, index string, err error) {
	dsl, index, err = elasticsql.Convert(sql)

	if err != nil {
		// Save history records only if query did not fail
		dsl, index, err = convertRetry(sql)
		if err != nil {
			return
		}
	}
	return
}

func convertRetry(sql string) (dsl, index string, err error) {
	var regTable = regexp.MustCompile("(?i)" + `FROM\s+(?P<table>[^ ,]+)|FROM\s+(?P<table>[^ ,]+)(?:\s*,\s*([^ ,]+))*\s+`)
	matches := regTable.FindStringSubmatch(sql)
	names := regTable.SubexpNames()
	for i, match := range matches {
		if i != 0 && names[i] == "table" && match != "" {
			dsl, _, err := elasticsql.Convert(strings.Replace(sql, match, "tmp", 1))
			if err == nil {
				return dsl, match, nil
			}
		}
	}
	return "", "", fmt.Errorf("parse table name failed : %s", sql)
}
