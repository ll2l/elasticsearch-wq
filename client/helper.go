package client

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

func unpack(s []string, vars ...*string) {
	for i, str := range s {
		*vars[i] = str
	}
}

func PrintPrettyMap(query map[string]interface{}) {
	b, err := json.Marshal(query)
	if err != nil {
		fmt.Println("json.Marshal failed:", err)
	}
	fmt.Println(string(b))
}

func (opts *RowsOptions) buildRowsQuery() io.Reader {
	var b strings.Builder

	b.WriteString("{\n")

	q := `"query" : { "match_all" : {} }`
	if opts.Where != "" {
		var field, op, value string
		unpack(strings.Split(opts.Where, "|"), &field, &op, &value)
		value = strings.Replace(value, "'", `"`, -1)
		q = `
	"query": {
		"bool": {
			"%s": {
				"%s": {
					%s: %s
				}
			}
		}
	}`
		switch op {
		case "=":
			q = fmt.Sprintf(q, "filter", "match_phrase", field, value)
		case "!=":
			q = fmt.Sprintf(q, "must_not", "match_phrase", field, value)
		case "LIKE":
			q = fmt.Sprintf(q, "filter", "match", field, value)
		case ">":
			value = fmt.Sprintf(`{"gt":%s}`, value)
			q = fmt.Sprintf(q, "filter", "range", field, value)
		case ">=":
			value = fmt.Sprintf(`{"gte":%s}`, value)
			q = fmt.Sprintf(q, "filter", "range", field, value)
		case "<":
			value = fmt.Sprintf(`{"lt":%s}`, value)
			q = fmt.Sprintf(q, "filter", "range", field, value)
		case "<=":
			value = fmt.Sprintf(`{"lte":%s}`, value)
			q = fmt.Sprintf(q, "filter", "range", field, value)
		case "IS NULL":
			q = fmt.Sprintf(q, "must_not", "exists", "\"field\"", field)
		case "IS NOT NULL":
			q = fmt.Sprintf(q, "must", "exists", "\"field\"", field)
		}
	}
	b.WriteString(q)

	if opts.SortColumn != "" {
		if opts.SortOrder == "" {
			opts.SortOrder = "ASC"
		}
		b.WriteString(",\n")
		b.WriteString(fmt.Sprintf(`"sort" : { "%s" : "%s"}`, opts.SortColumn, opts.SortOrder))
	}
	b.WriteString("\n}")
	fmt.Println(b.String())

	return strings.NewReader(b.String())
}
