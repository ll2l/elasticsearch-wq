package client

import (
	"github.com/elastic/go-elasticsearch/v6"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_dump(t *testing.T) {
	client, _ := elasticsearch.NewDefaultClient()
	es := Client{es: client}
	srcIndexName := "articles"
	dstIndexName := srcIndexName + "_copy"
	size := -1
	dumper := MigrateConfig{
		SrcEs:         &es,
		DstEs:         &es,
		SrcIndexName:  srcIndexName,
		DstIndexName:  dstIndexName,
		Size:          size,
		NumMigrations: 1200,
	}

	err := dumper.Migrate()
	assert.Equal(t, nil, err)

	//
	q := `
{
  "query": {
    "match_all": {}
  }
}
`
	es.ManageIndex(dstIndexName, "refresh")
	res, _ := es.Search(dstIndexName, q)
	assert.True(t, res.Hits.Total > 0, res)
}
