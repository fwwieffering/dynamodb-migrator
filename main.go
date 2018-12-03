package main

import (
	"errors"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

func defaultFlags() []cli.Flag {
	return []cli.Flag{
		cli.StringFlag{
			Name:   "source-table, st",
			Value:  "",
			Usage:  "Required: Dynamodb Table Name to source data from",
			EnvVar: "SOURCE_TABLE",
		},
		cli.StringFlag{
			Name:   "source-region, sr",
			Value:  "us-east-1",
			Usage:  "aws region of source table",
			EnvVar: "SOURCE_REGION",
		},
		cli.StringFlag{
			Name:   "source-account, sa",
			Value:  "",
			Usage:  "AWS Account number of the source table",
			EnvVar: "SOURCE_ACCOUNT",
		},
		cli.StringFlag{
			Name:   "destination-table, dt",
			Value:  "",
			Usage:  "Required: Dynamodb Table Name to write to",
			EnvVar: "DESTINATION_TABLE",
		},
		cli.StringFlag{
			Name:   "destination-region, dr",
			Value:  "us-east-1",
			Usage:  "aws region of destination table",
			EnvVar: "DESTINATION_REGION",
		},
		cli.StringFlag{
			Name:   "destination-account, da",
			Value:  "",
			Usage:  "AWS Account number of the destination table",
			EnvVar: "DESTINATION_ACCOUNT",
		},
		cli.StringFlag{
			Name:   "role, r",
			Value:  "",
			Usage:  "AWS Role Name to assume for source/dest accounts. Must be present in both accounts",
			EnvVar: "ROLE",
		},
		cli.StringFlag{
			Name:   "log-level",
			Value:  "error",
			Usage:  "Log level (panic, fatal, error, warn, info, or debug)",
			EnvVar: "PLUGIN_LOG_LEVEL,LOG_LEVEL",
		},
	}
}

func parseParams(c *cli.Context) error {
	logLevelString := c.String("log-level")
	logLevel, err := log.ParseLevel(logLevelString)
	if err != nil {
		return err
	}
	log.SetLevel(logLevel)

	// required flags
	if !c.IsSet("source-table") || !c.IsSet("destination-table") {
		return errors.New("source-table and destination-table flags are required")
	}

	// conditionally required flags
	anyCrossAccountFlagsSet := c.IsSet("source-account") || c.IsSet("destination-account") || c.IsSet("role")
	allCrossAccountFlagsSet := c.IsSet("source-account") && c.IsSet("destination-account") && c.IsSet("role")
	if anyCrossAccountFlagsSet && !allCrossAccountFlagsSet {
		return errors.New("If any of [source-account, destination-account, role] flags are provided all must be provided")
	}

	return nil
}

func initApp() *cli.App {
	app := cli.NewApp()
	app.Name = "dynamodb-migrator"
	app.Usage = "Migrate data from one dynamodb table to another"
	app.Version = fmt.Sprintf("0.1.0")

	app.Flags = defaultFlags()

	app.Action = cli.ActionFunc(defaultAction)

	app.Commands = []cli.Command{
		{
			Name:   "migrate",
			Action: defaultAction,
			Usage:  "Migrates data from Source Table to Destination Table",
			Flags:  defaultFlags(),
		},
		{
			Name:   "compare",
			Action: compare,
			Usage:  "Compares item counts from Source Table and Destination Table",
			Flags:  defaultFlags(),
		},
	}

	return app
}

func defaultAction(c *cli.Context) error {
	err := parseParams(c)

	if err != nil {
		return err
	}

	migrator := NewMigrator(&MigratorConfig{
		SourceTable:   c.String("source-table"),
		SourceRegion:  c.String("source-region"),
		SourceAccount: c.String("source-account"),
		DestTable:     c.String("destination-table"),
		DestRegion:    c.String("destination-region"),
		DestAccount:   c.String("destination-account"),
		Role:          c.String("role"),
	})
	return migrator.Migrate()
}

func compare(c *cli.Context) error {
	err := parseParams(c)

	if err != nil {
		return err
	}

	migrator := NewMigrator(&MigratorConfig{
		SourceTable:   c.String("source-table"),
		SourceRegion:  c.String("source-region"),
		SourceAccount: c.String("source-account"),
		DestTable:     c.String("destination-table"),
		DestRegion:    c.String("destination-region"),
		DestAccount:   c.String("destination-account"),
		Role:          c.String("role"),
	})

	return migrator.Compare()
}

func main() {
	app := initApp()

	app.Run(os.Args)
}
