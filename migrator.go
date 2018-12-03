package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	log "github.com/sirupsen/logrus"
)

// MigratorConfig holds the configuration for the migrator struct
type MigratorConfig struct {
	// SourceTable the source DynamoDB Table
	SourceTable string
	// SourceRegion the AWS Region of the SourceTable
	SourceRegion  string
	SourceAccount string
	DestTable     string
	DestRegion    string
	DestAccount   string
	Role          string
}

// Migrator executes the dynamodb migration
type Migrator struct {
	SourceTable *Table
	DestTable   *Table
	Config      *MigratorConfig
}

// NewMigrator generates a new Migrator struct with the provided MigratorConfig
func NewMigrator(c *MigratorConfig) *Migrator {
	log.Infoln("Generating AWS Credentials")
	var srcDdb *dynamodb.DynamoDB
	var destDdb *dynamodb.DynamoDB
	if len(c.Role) > 0 {
		log.Infoln(fmt.Sprintf("Assuming cross account role %s for accounts %s and %s", c.Role, c.SourceAccount, c.DestAccount))
		srcDdb = generateClientRole(c.SourceRegion, c.SourceAccount, c.Role)
		destDdb = generateClientRole(c.DestRegion, c.DestAccount, c.Role)
	} else {
		srcDdb = generateClient(c.SourceRegion)
		destDdb = generateClient(c.DestRegion)
	}
	return &Migrator{
		SourceTable: NewTable(srcDdb, c.SourceTable),
		DestTable:   NewTable(destDdb, c.DestTable),
		Config:      c,
	}
}

// Migrate Run runs the dynamodb migration from SourceTable to DestTable
func (m *Migrator) Migrate() error {
	waitChan := make(chan byte, 2)

	log.Infoln("Pulling items from source table...")
	go m.SourceTable.PullItems(waitChan)
	go m.DestTable.PushItems(m.SourceTable.ItemChan, waitChan)
	<-waitChan
	<-waitChan
	// get count of dest items
	m.DestTable.CountItems(waitChan)
	<-waitChan
	fmt.Println("--------------------------------------------------------------------------------------")
	fmt.Println("Migration complete.")
	fmt.Println(fmt.Sprintf("SOURCE TABLE items in %s: %d", m.SourceTable.Name, m.SourceTable.ItemCount.Count))
	fmt.Println(fmt.Sprintf("DESTINATION TABLE Items in %s: %d", m.DestTable.Name, m.DestTable.ItemCount.Count))

	return nil
}

// Compare checks Item count in source and destination table
func (m *Migrator) Compare() error {
	waitChan := make(chan byte, 2)
	log.Infoln("Pulling items from source table and destination tables...")
	go m.SourceTable.CountItems(waitChan)
	go m.DestTable.CountItems(waitChan)
	<-waitChan
	<-waitChan
	fmt.Println("--------------------------------------------------------------------------------------")
	fmt.Println("Compare complete")
	fmt.Println(fmt.Sprintf("SOURCE TABLE items in %s: %d", m.SourceTable.Name, m.SourceTable.ItemCount.Count))
	fmt.Println(fmt.Sprintf("DESTINATION TABLE Items in %s: %d", m.DestTable.Name, m.DestTable.ItemCount.Count))

	return nil
}
