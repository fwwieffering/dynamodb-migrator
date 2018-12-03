package main

import (
	"flag"
	"testing"

	"github.com/urfave/cli"
)

func TestDefaultFlags(t *testing.T) {
	defaultFlags()
}

func TestParseParams(t *testing.T) {
	flags := flag.NewFlagSet("test", 0)
	c := cli.NewContext(cli.NewApp(), flags, nil)
	err := parseParams(c)
	if err == nil {
		t.Fatalf("parseParams should fail when the log level can't parse")
	}

	flags.String("log-level", "info", "")
	c = cli.NewContext(cli.NewApp(), flags, nil)
	err = parseParams(c)
	if err == nil {
		t.Fatalf("parseParams should return an error when source-table and destination-table are unset")
	}

	flags.String("source-table", "foo", "")
	flags.Set("source-table", "ok")
	flags.String("destination-table", "bar", "")
	flags.Set("destination-table", "ok")
	flags.String("source-account", "foobar", "")
	flags.Set("source-account", "ok")
	c = cli.NewContext(cli.NewApp(), flags, nil)
	err = parseParams(c)
	if err == nil {
		t.Fatalf("parseParams should return an error when not all cross-account vars are set")
	}

	flags.String("destination-account", "oops", "ah")
	flags.Set("destination-account", "boo")
	flags.String("role", "hey", "ya")
	flags.Set("role", "myrole")
	c = cli.NewContext(cli.NewApp(), flags, nil)
	err = parseParams(c)
	if err != nil {
		t.Fatalf("parseParams should not return an error when all vars are set")
	}

}
