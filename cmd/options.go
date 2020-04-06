package cmd

import (
	"os"
	"os/user"

	"github.com/jessevdk/go-flags"
)

type Options struct {
	Version                      bool   `short:"v" long:"version" description:"Print version"`
	Debug                        bool   `short:"d" long:"debug" description:"Enable debugging mode"`
	Address                      string `long:"address" description:"Server http url" default:"http://localhost:9200"`
	Name                         string `long:"name" description:"es cluster alias" default:"default"`
	User                         string `long:"user" description:"Database user"`
	Pass                         string `long:"pass" description:"Password for user"`
	HTTPHost                     string `long:"bind" description:"HTTP server host" default:"localhost"`
	HTTPPort                     uint   `long:"listen" description:"HTTP server listen port" default:"8081"`
	AuthUser                     string `long:"auth-user" description:"HTTP basic auth user"`
	AuthPass                     string `long:"auth-pass" description:"HTTP basic auth password"`
	SkipOpen                     bool   `short:"s" long:"skip-open" description:"Skip browser open on start"`
	Sessions                     bool   `long:"sessions" description:"Enable multiple database sessions"`
	ReadOnly                     bool   `long:"readonly" description:"Run database connection in readonly mode"`
	LockSession                  bool   `long:"lock-session" description:"Lock session to a single database connection"`
	Bookmark                     string `short:"b" long:"bookmark" description:"Bookmark to use for connection. Bookmark files are stored under $HOME/.pgweb/bookmarks/*.toml" default:""`
	BookmarksDir                 string `long:"bookmarks-dir" description:"Overrides default directory for bookmark files to search" default:""`
	DisableConnectionIdleTimeout bool   `long:"no-idle-timeout" description:"Disable connection idle timeout"`
	ConnectionIdleTimeout        int    `long:"idle-timeout" description:"Set connection idle timeout in minutes" default:"180"`
	Prefix                       string `long:"prefix" description:"Add a url prefix"`
	DisablePrettyJSON            bool   `long:"no-pretty-json" description:"Disable JSON formatting feature for result export"`
}

var Opts Options

// ParseOptions returns a new options struct from the input arguments
func ParseOptions(args []string) (Options, error) {
	var opts = Options{}

	_, err := flags.ParseArgs(&opts, args)
	if err != nil {
		return opts, err
	}

	if opts.Prefix == "" {
		opts.Prefix = os.Getenv("URL_PREFIX")
	}

	if os.Getenv("SESSIONS") != "" {
		opts.Sessions = true
	}

	if os.Getenv("LOCK_SESSION") != "" {
		opts.LockSession = true
		opts.Sessions = false
	}

	if opts.Sessions {
		opts.Bookmark = ""
		opts.Address = ""
		opts.User = ""
		opts.Pass = ""
	}

	if opts.AuthUser == "" && os.Getenv("AUTH_USER") != "" {
		opts.AuthUser = os.Getenv("AUTH_USER")
	}

	if opts.AuthPass == "" && os.Getenv("AUTH_PASS") != "" {
		opts.AuthPass = os.Getenv("AUTH_PASS")
	}

	return opts, nil
}

// SetDefaultOptions parses and assigns the options
func SetDefaultOptions() error {
	opts, err := ParseOptions([]string{})
	if err != nil {
		return err
	}
	Opts = opts
	return nil
}

// GetCurrentUser returns a current user name
func GetCurrentUser() string {
	u, _ := user.Current()
	if u != nil {
		return u.Username
	}
	return os.Getenv("USER")
}
