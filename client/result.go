package client

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v6/esapi"
	"sort"
	"strings"
	"time"
)

var DisablePrettyJSON = false

type searchResponse struct {
	ScrollID string `json:"_scroll_id"`
	Took     int    `json:"took"`
	Timeout  int    `json:"time_out"`
	Hits     struct {
		MaxScore float64 `json:"max_score"`
		Total    int     `json:"took"`
		Hits     []struct {
			ID     string                 `json:"_id"`
			Index  string                 `json:"_index"`
			Type   string                 `json:"_type"`
			Score  float64                `json:"_score"`
			Source map[string]interface{} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

type RowsOptions struct {
	Where      string // Custom filter
	Offset     int    // Number of rows to skip
	Limit      int    // Number of rows to fetch
	SortColumn string // Column to sort by
	SortOrder  string // Sort direction (ASC, DESC)
}

type Pagination struct {
	Rows    int64 `json:"rows_count"`
	Page    int64 `json:"page"`
	Pages   int64 `json:"pages_count"`
	PerPage int64 `json:"per_page"`
}

type Row []interface{}

type Table struct {
	Columns    []string    `json:"columns"`
	Rows       []Row       `json:"rows"`
	Pagination *Pagination `json:"pagination,omitempty"`
}

func (r *searchResponse) IsEmpty() bool {
	if r != nil {
		return len(r.Hits.Hits) < 1
	}
	return true
}

func (r *searchResponse) SourceFields() []string {
	var f []string
	hit := r.Hits.Hits[0]
	for k := range hit.Source {
		f = append(f, k)
	}
	return f
}

func (r *searchResponse) AsTableRows() *Table {
	var t Table
	if r.IsEmpty() {
		return &t
	}

	fields := r.SourceFields()
	sort.Strings(fields)

	for _, row := range r.Hits.Hits {
		i := make([]interface{}, 0, len(row.Source))
		for _, v := range fields {
			i = append(i, row.Source[v])
		}
		t.Rows = append(t.Rows, i)
	}
	t.Columns = fields

	return &t
}

type elasticErrResp struct {
	Error struct {
		RootCause []struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		} `json:"root_cause"`
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"error"`
	Status int `json:"status"`
}

func checkElasticResp(res *esapi.Response, eserr error) error {
	if eserr != nil {
		return eserr
	}

	if res.IsError() {
		var e elasticErrResp
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			return err
		}
		var rootCause []string
		for _, r := range e.Error.RootCause {
			rootCause = append(rootCause, fmt.Sprintf("%s: %s", r.Type, r.Reason))
		}
		return fmt.Errorf(strings.Join(rootCause, "\n"))
	}
	return nil
}

func (res *Table) Format() []map[string]interface{} {
	var items []map[string]interface{}

	for _, row := range res.Rows {
		item := make(map[string]interface{})

		for i, c := range res.Columns {
			item[c] = row[i]
		}

		items = append(items, item)
	}

	return items
}

func (res *Table) CSV(withHeader bool) []byte {
	buff := &bytes.Buffer{}
	writer := csv.NewWriter(buff)

	buff.WriteString("\xEF\xBB\xBF") // UTF-8 BOM
	if withHeader {
		writer.Write(res.Columns)
	}

	for _, row := range res.Rows {
		record := make([]string, len(res.Columns))

		for i, item := range row {
			if item != nil {
				switch v := item.(type) {
				case time.Time:
					record[i] = v.Format("2006-01-02 15:04:05")
				default:
					record[i] = fmt.Sprintf("%v", item)
				}
			} else {
				record[i] = ""
			}
		}

		err := writer.Write(record)

		if err != nil {
			fmt.Println(err)
			break
		}
	}

	writer.Flush()
	return buff.Bytes()
}

func (res *Table) JSON() []byte {
	var data []byte

	if DisablePrettyJSON {
		data, _ = json.Marshal(res.Format())
	} else {
		data, _ = json.MarshalIndent(res.Format(), "", " ")
	}

	return data
}
