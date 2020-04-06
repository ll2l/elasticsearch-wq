package bookmarks

import (
	"fmt"
)

// Bookmark contains information about bookmarked cluster connection
type Bookmark struct {
	Addresses []string `json:"addresses"`
	User      string   `json:"user"`
	Password  string   `json:"password"`
	Alias     string   `json:"alias"`
	Kibana    string   `json:"kibana"`
}

var Clusters = map[string]Bookmark{}

func GetClusterConfig(clusterName string) (Bookmark, error) {
	conf, ok := Clusters[clusterName]
	if !ok {
		return Bookmark{}, fmt.Errorf("couldn't find a config with name %s", clusterName)
	}
	return conf, nil
}

// return all cluster name
func GetBookmarks() []string {
	c := make([]string, 0, len(Clusters))
	for k := range Clusters {
		c = append(c, k)
	}
	return c

}

// return all cluster name
func GetKibanaUrlByAlias(alias string) string {
	for _, v := range Clusters {
		if v.Alias == alias {
			return v.Kibana
		}
	}
	return ""
}
