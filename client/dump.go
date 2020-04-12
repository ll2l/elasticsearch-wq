package client

import (
	"bytes"
	"encoding/json"
	"github.com/dustin/go-humanize"
	"github.com/elastic/go-elasticsearch/v6"
	"log"
	"strings"
	"time"

	"fmt"
	"io"
)

// Dump represents a database dump
type Dump struct {
	Index  string
	Fields []string
}

type bulkResponse struct {
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

func (d *Dump) Export(c *Client, writer io.Writer) error {
	var (
		scrollID string
		r        searchResponse
		size     = 100
	)

	res, err := c.es.Search(
		c.es.Search.WithIndex(d.Index),
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

func (d *Dump) Migrate(c *Client, dstHost, dstUser, dstPass, dstIndex string, numItems int) error {
	var (
		scrollID string
		r        searchResponse
		size     = 100
	)

	// prepare dst
	dstEsClient, err := NewFromParams(dstHost, "migrateDstHost", dstUser, dstPass)
	if err != nil {
		return err
	}

	srcIndexMapping, err := c.Mapping(d.Index)
	if err != nil {
		return err
	}

	if err = prepareDstIndex(srcIndexMapping[d.Index], dstEsClient, dstIndex); err != nil {
		return err
	}

	blkQueue := make(chan searchResponse, 4)
	go bulkIndexer(blkQueue, dstEsClient, dstIndex, size)

	// numItems > maxSize: use scroll api
	res, err := c.es.Search(
		c.es.Search.WithIndex(d.Index),
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

	scrollID = r.ScrollID
	res.Body.Close()

	blkQueue <- r

	numScrolled := len(r.Hits.Hits)
	for {
		var sr searchResponse
		res, err := c.es.Scroll(
			c.es.Scroll.WithScrollID(scrollID),
			c.es.Scroll.WithScroll(time.Minute))

		if err := checkElasticResp(res, err); err != nil {
			log.Printf("Error scroll: %c", err)
			return err
		}

		if err := json.NewDecoder(res.Body).Decode(&sr); err != nil {
			log.Printf("Error parsing the response body: %c", err)
			close(blkQueue)
			return err
		}
		numScrolled += len(sr.Hits.Hits)

		if sr.IsEmpty() {
			log.Println(">Finished scrolling")
			close(blkQueue)
			break
		}

		if numItems > 0 && numScrolled >= numItems {
			if numScrolled > numItems {
				sr.Hits.Hits = sr.Hits.Hits[:numItems+size-numScrolled]
			}
			blkQueue <- sr
			close(blkQueue)
			log.Println(">>>Finished scrolling")
			break
		}

		blkQueue <- sr
		res.Body.Close()
	}

	return nil
}

func bulkIndexer(ch chan searchResponse, client *Client, indexName string, size int) {
	var (
		buf        bytes.Buffer
		numErrors  int
		numIndexed int
	)

	batch := 1000
	es := client.es

	start := time.Now().UTC()

	// Loop over the collection
	count := 0
	for r := range ch {
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
			if count >= batch || len(r.Hits.Hits) < size {
				batchIndexed, batchErrors := bulker(es, buf, indexName)
				numIndexed += batchIndexed
				numErrors += batchErrors
				// Reset the buffer and items counter
				count = 0
				buf.Reset()
			}
		}
	}

	if buf.Len() > 0 {
		batchIndexed, batchErrors := bulker(es, buf, indexName)
		numIndexed += batchIndexed
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

func bulker(es *elasticsearch.Client, buf bytes.Buffer, index string) (int, int) {
	var (
		raw        map[string]interface{}
		numErrors  int
		numIndexed int
		blk        *bulkResponse
	)

	res, err := es.Bulk(bytes.NewReader(buf.Bytes()), es.Bulk.WithIndex(index), es.Bulk.WithDocumentType("documents"))
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

func prepareDstIndex(mapping interface{}, dstEsClient *Client, dstIndexName string) error {
	res, err := dstEsClient.es.Indices.Delete([]string{dstIndexName})
	if err := checkElasticResp(res, err); err != nil {
		if !strings.Contains(err.Error(), "no such index") {
			return fmt.Errorf("cannot delete index: %s", err)
		}
	}

	mJson, err := json.Marshal(mapping)
	if err != nil {
		return fmt.Errorf("cannot marshal mapping: %s", err)
	}

	var buf bytes.Buffer
	buf.Write(mJson)
	res, err = dstEsClient.es.Indices.Create(dstIndexName, dstEsClient.es.Indices.Create.WithBody(&buf))
	if err := checkElasticResp(res, err); err != nil {
		return fmt.Errorf("cannot create index: %s", err)
	}

	return nil
}
