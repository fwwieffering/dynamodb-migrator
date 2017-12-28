package main

import (
	"log"
	"os"
)

func main() {
	srcTableName, present := os.LookupEnv("SOURCE_TABLE")
	if !present {
		panic("SOURCE_TABLE environment variable must be defined")
	}
	destTableName, present := os.LookupEnv("DESTINATION_TABLE")
	if !present {
		panic("DESTINATION_TABLE environment variable must be defined")
	}
	region, present := os.LookupEnv("AWS_REGION")
	if !present {
		region = "us-east-1"
	}
	waitChan := make(chan byte, 2)
	srcDdb, destDdb := generateClients(region)
	srcTable := NewTable(srcDdb, srcTableName)
	destTable := NewTable(destDdb, destTableName)
	log.Printf("Dynamodb migration from %s to %s", srcTable.Name, destTable.Name)
	log.Printf("Increasing table %s write capacity to 3000 for table and all GSIs", destTable.Name)
	destOrigSettings := destTable.IncreaseCapacity("write")
	log.Printf("Increasing table %s read capacity to 3000 for table and all GSIs", srcTable.Name)
	srcOrigSettings := srcTable.IncreaseCapacity("read")
	log.Println("Pulling items from source table")
	go srcTable.PullItems(waitChan)
	go destTable.PushItems(srcTable.ItemChan, waitChan)
	<-waitChan
	<-waitChan
	// get count of dest items
	destTable.PullItems(waitChan)
	<-waitChan
	log.Printf("Migration complete.")
	log.Printf("Items in %s: %d", srcTableName, srcTable.ItemCount)
	log.Printf("Items in %s: %d", destTable.Name, destTable.ItemCount)
	log.Printf("Knocking table %s write capacity back down to original settings", destTable.Name)
	destTable.UpdateTable(destOrigSettings)
	log.Printf("Knocking table %s read capacity back down to original settings", srcTable.Name)
	srcTable.UpdateTable(srcOrigSettings)
}
