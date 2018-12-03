package main

import (
	"log"

	"github.com/aws/aws-sdk-go/service/dynamodb"
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
	Config *MigratorConfig
}

// Run runs the dynamodb migration
func (m *Migrator) Run() error {
	waitChan := make(chan byte, 2)

	var srcDdb *dynamodb.DynamoDB
	var destDdb *dynamodb.DynamoDB
	if len(m.Config.Role) > 0 {
		srcDdb = generateClientRole(m.Config.SourceRegion, m.Config.SourceAccount, m.Config.Role)
		destDdb = generateClientRole(m.Config.DestRegion, m.Config.DestAccount, m.Config.Role)
	} else {
		srcDdb = generateClient(m.Config.SourceRegion)
		destDdb = generateClient(m.Config.DestRegion)
	}
	srcTable := NewTable(srcDdb, m.Config.SourceTable)
	destTable := NewTable(destDdb, m.Config.DestTable)

	log.Println("Pulling items from source table")
	go srcTable.PullItems(waitChan)
	go destTable.PushItems(srcTable.ItemChan, waitChan)
	<-waitChan
	<-waitChan
	// get count of dest items
	destTable.PullItems(waitChan)
	<-waitChan
	log.Printf("Migration complete.")
	log.Printf("Items in %s: %d", m.Config.SourceTable, srcTable.ItemCount)
	log.Printf("Items in %s: %d", m.Config.DestTable, destTable.ItemCount)

	return nil
}
