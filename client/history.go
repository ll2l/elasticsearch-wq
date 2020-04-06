package client

import (
	"time"
)

type Record struct {
	Query     string `json:"query"`
	Timestamp string `json:"timestamp"`
}

func newHistory() []Record {
	return make([]Record, 0)
}

func newHistoryRecord(query string) Record {
	return Record{
		Query:     query,
		Timestamp: time.Now().String(),
	}
}
