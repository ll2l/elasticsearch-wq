package cmd

import (
	"fmt"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/jessevdk/go-flags"

	"github.com/ll2l/esweb/api"
	"github.com/ll2l/esweb/bookmarks"
	"github.com/ll2l/esweb/client"
)

var (
	options         Options
	readonlyWarning = `
------------------------------------------------------
SECURITY WARNING: You are running pgweb in read-only mode.
This mode is designed for environments where users could potentially delete / change data.
For proper read-only access please follow postgresql role management documentation.
------------------------------------------------------`
)

func exitWithMessage(message string) {
	fmt.Println("Error:", message)
	os.Exit(1)
}

func printVersion() {
	fmt.Println("esweb v0.0.1")
}

func initBookmarks() {
	bookmarksDir := options.BookmarksDir
	if bookmarksDir == "" {
		bookmarksDir, _ = os.Getwd()
	}
	viper.SetConfigName("bookmark")
	viper.AddConfigPath(bookmarksDir)
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			bookmarks.Clusters["default"] = bookmarks.Bookmark{Addresses: []string{client.DefaultCluster}}
			log.Printf("bookmark config file not found, using default address %s", client.DefaultCluster)
			// Config file not found; ignore error if desired
		} else {
			log.Fatal("bookmark parser error")
			// Config file was found but another error was produced
		}
	}

	all := viper.AllSettings()
	for name, conf := range all {
		B := bookmarks.Bookmark{}
		if err := mapstructure.Decode(conf, &B); err != nil {
			log.Printf("Parser %s config failed: %s", name, err)
		}
		bookmarks.Clusters[name] = B
	}
}
func initClientUsingBookmark(bookmark string) (*client.Client, error) {
	conf, err := bookmarks.GetClusterConfig(bookmark)
	if err != nil {
		return nil, err
	}
	cl, err := client.NewFromBookmarks(conf)
	if err != nil {
		return nil, err
	}

	if !cl.Alive() {
		return nil, fmt.Errorf("cluster:%s is not alive", bookmark)
	}

	if Opts.Debug {
		log.Printf("Cluster:%s connected", bookmark)
	}

	return cl, nil
}

func initClient() {
	if options.Address == "" && options.Bookmark == "" {
		return
	}

	var cl *client.Client
	var err error

	if options.Bookmark != "" {
		cl, err = initClientUsingBookmark(options.Bookmark)
	} else {
		cl, err = client.New()
	}
	if err != nil {
		exitWithMessage(err.Error())
	}

	fmt.Println("Connecting to server...")
	_, err = cl.Info()
	if err != nil {
		exitWithMessage(err.Error())
	}

	api.EsClient = cl
}

func initOptions() {
	opts, err := ParseOptions(os.Args)
	if err != nil {
		switch err.(type) {
		case *flags.Error:
			// no need to print error, flags package already does that
		default:
			fmt.Println(err.Error())
		}
		os.Exit(1)
	}
	Opts = opts
	options = opts

	if options.Address != "" {
		client.DefaultCluster = options.Address
		client.DefaultUser = options.User
		client.DefaultPassword = options.Pass
	}

	if options.Name != "" {
		client.DefaultAlias = options.Name
	}

	if options.Version {
		printVersion()
		os.Exit(0)
	}

	if options.ReadOnly {
		fmt.Println(readonlyWarning)
	}

	client.DisablePrettyJSON = options.DisablePrettyJSON

	printVersion()
}

func startServer() {
	router := gin.Default()

	// Enable HTTP basic authentication only if both user and password are set
	if options.AuthUser != "" && options.AuthPass != "" {
		auth := map[string]string{options.AuthUser: options.AuthPass}
		router.Use(gin.BasicAuth(auth))
	}

	api.SetupRoutes(router)

	fmt.Println("Starting server...")
	go func() {
		err := router.Run(fmt.Sprintf("%v:%v", options.HTTPHost, options.HTTPPort))
		if err != nil {
			fmt.Println("Cant start server:", err)
			if strings.Contains(err.Error(), "address already in use") {
				openPage()
			}
			os.Exit(1)
		}
	}()
}

func handleSignals() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	<-c
}

func openPage() {
	url := fmt.Sprintf("http://%v:%v/%s", options.HTTPHost, options.HTTPPort, options.Prefix)
	fmt.Println("To view database open", url, "in browser")

	if options.SkipOpen {
		return
	}

	_, err := exec.Command("which", "open").Output()
	if err != nil {
		return
	}

	exec.Command("open", url).Output()
}

func Run() {
	initOptions()
	initBookmarks()
	initClient()

	if !options.Debug {
		gin.SetMode("release")
	}

	startServer()
	openPage()
	handleSignals()
}
