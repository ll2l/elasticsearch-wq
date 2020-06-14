package client

import (
	"bytes"
	"encoding/json"
	"github.com/dustin/go-humanize"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fmt"
	"io"
)

// Dump represents a database dump
type MigrateConfig struct {
	DocChan       chan Scroll
	MiddleCh      chan Scroll
	Closing       chan string
	Closed        chan struct{}
	SrcIndexName  string
	DstIndexName  string
	Fields        []string
	SrcEs         *Client
	DstEs         *Client
	Size          int
	Workers       int
	NumScrolled   int64
	NumBulked     int64
	NumMigrations int64
}

type Scroll struct {
	searchResponse
	ScrollID string `json:"_scroll_id"`
}

type BulkResp struct {
	Errors bool `json:"errors"`
	Items  []struct {
		Index struct {
			ID     string `json:"_id"`
			Result string `json:"result"`
			Status int    `json:"status"`
			Error  struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
				Cause  struct {
					Type   string `json:"type"`
					Reason string `json:"reason"`
				} `json:"caused_by"`
			} `json:"error"`
		} `json:"index"`
	} `json:"items"`
}

func (mc *MigrateConfig) Export(c *Client, writer io.Writer) error {
	var (
		scrollID string
		r        searchResponse
		size     = 100
	)

	res, err := c.es.Search(
		c.es.Search.WithIndex(mc.SrcIndexName),
		c.es.Search.WithSort("_doc"),
		c.es.Search.WithSize(size),
		c.es.Search.WithScroll(time.Minute),
	)

	if err := checkElasticResp(res, err); err != nil {
		return err
	}

	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Printf("Error parsing the response body: %c", err)
		return err
	}
	res.Body.Close()

	scrollID = r.ScrollID

	// Handle the first batch of data and extract the scrollID
	buff := &bytes.Buffer{}
	buff.WriteString("\xEF\xBB\xBF")
	buff.Write(r.AsTableRows().CSV(true))
	writer.Write(buff.Bytes())
	buff.Reset()

	for {
		// (scroll example) https://github.com/elastic/go-elasticsearch/issues/44
		res, err := c.es.Scroll(c.es.Scroll.WithScrollID(scrollID), c.es.Scroll.WithScroll(time.Minute))
		if err := checkElasticResp(res, err); err != nil {
			return err
		}

		if r.IsEmpty() {
			log.Println("Finished scrolling")
			break
		} else {
			if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
				log.Printf("Error parsing the response body: %c", err)
				return err
			}
			writer.Write(r.AsTableRows().CSV(false))
			buff.Reset()
			res.Body.Close()
		}
	}
	return nil
}

func (mc *MigrateConfig) Stop(by string) {
	select {
	case mc.Closing <- by:
		<-mc.Closed
	case <-mc.Closed:
	}
}

func (mc *MigrateConfig) Init(wg *sync.WaitGroup) {
	if mc.Workers == 0 {
		mc.Workers = 10
	}

	mc.DocChan = make(chan Scroll, 1000)
	mc.MiddleCh = make(chan Scroll, 1000)
	mc.Closing = make(chan string)
	mc.Closed = make(chan struct{})

	for i := 0; i < mc.Workers; i++ {
		wg.Add(1)
		go func() {
			mc.Bulk()
			wg.Done()
		}()
	}
}

func (mc *MigrateConfig) Migrate() error {
	wg := sync.WaitGroup{}

	if err := mc.CreateDstIndex(); err != nil {
		return err
	}

	mc.Init(&wg)
	var stoppedBy string

	sc, total, err := mc.NewSlicedScroll()
	if err != nil {
		return fmt.Errorf("error scroll: %s", err)
	}

	if mc.NumMigrations > 0 {
		total = mc.NumMigrations
	}

	// middle goroutine
	go func() {
		exit := func(v Scroll, needSend bool) {
			close(mc.Closed)
			if needSend {
				mc.DocChan <- v
			}
			close(mc.DocChan)
		}

		for {
			select {
			case stoppedBy = <-mc.Closing:
				exit(Scroll{}, false)
				return
			case v := <-mc.MiddleCh:
				if atomic.LoadInt64(&mc.NumScrolled) >= total {
					exit(v, true)
					return
				}
				select {
				case stoppedBy = <-mc.Closing:
					exit(v, true)
					return
				case mc.DocChan <- v:
				}
			}
		}

	}()

	for _, i := range sc {
		mc.MiddleCh <- i
		atomic.AddInt64(&mc.NumScrolled, int64(len(i.Hits.Hits)))
		go func(sid string) {
			for mc.NextScroll(sid) == false {
			}
		}(i.ScrollID)
	}

	// monitor scrolled doc and stop
	go func() {
		for {
			numScrolled := atomic.LoadInt64(&mc.NumScrolled)
			if numScrolled >= total {
				mc.Stop("stop-> reach the total")
				break
			}
		}
	}()

	wg.Wait()
	fmt.Printf("numItems: %d, numIndexed: %d", atomic.LoadInt64(&mc.NumScrolled), atomic.LoadInt64(&mc.NumBulked))
	fmt.Println("stop by", stoppedBy)

	return nil
}

func (mc *MigrateConfig) NewSlicedScroll() ([]Scroll, int64, error) {
	var (
		scrolls   []Scroll
		total     int64
		maxSliced = 5
	)

	dsl := `{
    "slice": {
        "id": %d,
        "max": %d
    },
    "query": {
        "match_all" : {}
    }}`
	for i := 0; i < maxSliced; i++ {
		var buf bytes.Buffer
		buf.WriteString(fmt.Sprintf(dsl, i, maxSliced))
		res, err := mc.SrcEs.es.Search(
			mc.SrcEs.es.Search.WithIndex(mc.SrcIndexName),
			mc.SrcEs.es.Search.WithBody(&buf),
			mc.SrcEs.es.Search.WithSort("_doc"),
			mc.SrcEs.es.Search.WithSize(100),
			mc.SrcEs.es.Search.WithScroll(time.Minute),
		)

		if err := checkElasticResp(res, err); err != nil {
			return nil, 0, err
		}

		scroll := Scroll{}
		if err := json.NewDecoder(res.Body).Decode(&scroll); err != nil {
			return nil, 0, err
		}
		total += int64(scroll.Hits.Total)
		scrolls = append(scrolls, scroll)

		res.Body.Close()

	}

	return scrolls, total, nil
}

func (mc *MigrateConfig) NextScroll(sid string) (done bool) {

	res, err := mc.SrcEs.es.Scroll(
		mc.SrcEs.es.Scroll.WithScrollID(sid),
		mc.SrcEs.es.Scroll.WithScroll(time.Minute))

	if err := checkElasticResp(res, err); err != nil {
		log.Printf("Error scroll: %s", err)
		return true
	}
	defer res.Body.Close()

	var r Scroll
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Printf("Error parsing the response body: %s", err)
		return true
	}

	if r.IsEmpty() {
		log.Println(">Finished scrolling")
		return true
	}

	select {
	case <-mc.Closed:
		return true
	default:
	}

	select {
	case <-mc.Closed:
		return true
	case mc.MiddleCh <- r:
		atomic.AddInt64(&mc.NumScrolled, int64(len(r.Hits.Hits)))
	}

	return false
}

func (mc *MigrateConfig) Bulk() {
	var (
		buf        bytes.Buffer
		numErrors  int
		numIndexed int
	)

	batch := 1000
	start := time.Now().UTC()

	// Loop over the collection
	count := 0
	for r := range mc.DocChan {
		for _, a := range r.Hits.Hits {
			count++

			// Prepare the metadata payload
			meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%s" } }%s`, a.ID, "\n"))
			// fmt.Printf("%s", meta)

			// Prepare the data payload: encode article to JSON
			data, err := json.Marshal(a.Source)
			if err != nil {
				log.Fatalf("Cannot encode article %s: %s", a.ID, err)
			}

			// Append newline to the data payload
			data = append(data, "\n"...) // <-- Comment out to trigger failure for batch

			buf.Grow(len(meta) + len(data))
			buf.Write(meta)
			buf.Write(data)
			// When a threshold is reached, execute the Bulk() request with body from buffer
			if count >= batch {
				batchIndexed, batchErrors := mc.bulk(buf, mc.DstIndexName)
				numIndexed += batchIndexed
				numErrors += batchErrors
				// Reset the buffer and items counter
				count = 0
				atomic.AddInt64(&mc.NumBulked, int64(numIndexed))
				buf.Reset()
			}
		}
	}

	if buf.Len() > 0 {
		batchIndexed, batchErrors := mc.bulk(buf, mc.DstIndexName)
		numIndexed += batchIndexed
		atomic.AddInt64(&mc.NumBulked, int64(numIndexed))
		numErrors += batchErrors
	}

	// Report the results: number of indexed docs, number of errors, duration, indexing rate
	//
	fmt.Print("\n")
	log.Println(strings.Repeat("▔", 65))

	dur := time.Since(start)

	if numErrors > 0 {
		log.Fatalf(
			"Indexed [%s] documents with [%s] errors in %s (%s docs/sec)",
			humanize.Comma(int64(numIndexed)),
			humanize.Comma(int64(numErrors)),
			dur.Truncate(time.Millisecond),
			humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(numIndexed))),
		)
	} else {
		log.Printf(
			"Sucessfuly indexed [%s] documents in %s (%s docs/sec)",
			humanize.Comma(int64(numIndexed)),
			dur.Truncate(time.Millisecond),
			humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(numIndexed))),
		)
	}

}

func (mc *MigrateConfig) bulk(buf bytes.Buffer, index string) (int, int) {
	var (
		raw        map[string]interface{}
		numErrors  int
		numIndexed int
		blk        *BulkResp
	)

	res, err := mc.DstEs.es.Bulk(
		bytes.NewReader(buf.Bytes()),
		mc.DstEs.es.Bulk.WithIndex(index),
		mc.DstEs.es.Bulk.WithDocumentType("documents"),
	)
	if err != nil {
		log.Fatalf("Failure indexing %s", err)
	}
	defer res.Body.Close()

	// If the whole request failed, print error and mark all documents as failed
	//
	if res.IsError() {
		if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
			log.Fatalf("Failure to to parse response body: %s", err)
		} else {
			log.Printf("  Error: [%d] %s: %s",
				res.StatusCode,
				raw["error"].(map[string]interface{})["type"],
				raw["error"].(map[string]interface{})["reason"],
			)
		}
		// A successful response might still contain errors for particular documents...
		//
	} else {
		if err := json.NewDecoder(res.Body).Decode(&blk); err != nil {
			log.Fatalf("Failure to to parse response body: %s", err)
		} else {
			for _, d := range blk.Items {
				// ... so for any HTTP status above 201 ...
				//
				if d.Index.Status > 201 {
					// ... increment the error counter ...
					//
					numErrors++

					// ... and print the response status and error information ...
					log.Printf("  Error: [%d]: %s: %s: %s: %s",
						d.Index.Status,
						d.Index.Error.Type,
						d.Index.Error.Reason,
						d.Index.Error.Cause.Type,
						d.Index.Error.Cause.Reason,
					)
				} else {
					// ... otherwise increase the success counter.
					//
					numIndexed++
				}
			}
		}
	}

	return numIndexed, numErrors
}

func (mc *MigrateConfig) CreateDstIndex() error {
	body, err := mc.GetSrcIndexSettings()
	if err != nil {
		return fmt.Errorf("error get index settings: %s", err)
	}

	res, err := mc.DstEs.es.Indices.Exists([]string{mc.DstIndexName})
	if err != nil {
		return fmt.Errorf("error check exisits: %s", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 200 {
		err := mc.DstEs.ManageIndex(mc.DstIndexName, "delete")
		if err != nil {
			return fmt.Errorf("cannot delete index: %s", err)
		}
	}

	mJson, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("cannot marshal body: %s", err)
	}

	var buf bytes.Buffer
	buf.Write(mJson)
	res, err = mc.DstEs.es.Indices.Create(mc.DstIndexName, mc.DstEs.es.Indices.Create.WithBody(&buf))
	if err := checkElasticResp(res, err); err != nil {
		return fmt.Errorf("cannot create index: %s", err)
	}

	return nil
}

func (mc *MigrateConfig) GetSrcIndexSettings() (map[string]interface{}, error) {
	m, err := mc.SrcEs.Mapping(mc.SrcIndexName)
	if err != nil {
		return nil, err
	}

	s, err := mc.SrcEs.Settings(mc.SrcIndexName)
	if err != nil {
		return nil, err
	}

	invalidFields := []string{
		"provided_name",
		"creation_date",
		"uuid",
		"version",
	}

	body := make(map[string]interface{})
	body["mappings"] = m[mc.SrcIndexName].(map[string]interface{})["mappings"]
	body["settings"] = s[mc.SrcIndexName].(map[string]interface{})["settings"]

	for _, f := range invalidFields {
		delete(body["settings"].(map[string]interface{})["index"].(map[string]interface{}), f)
	}
	return body, nil
}
