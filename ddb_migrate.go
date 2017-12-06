package main

import (
	"errors"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func pullItems(client *dynamodb.DynamoDB, srcTable string, itemChan chan []map[string]*dynamodb.AttributeValue, waitChan chan byte) {
	// func pullItems(client *dynamodb.DynamoDB, srcTable string, itemChan chan []map[string]*dynamodb.AttributeValue) {
	// paginate
	lockChan := make(chan byte)
	// dynamodb scanners
	shards := 5

	for i := 0; i < shards; i++ {
		go scanTable(client, srcTable, i, shards, itemChan, lockChan)
	}
	cleanUpThreads("PullItems", shards, lockChan)
	close(itemChan)
	log.Println("Item channel closed")
	waitChan <- 1
}

func scanTable(client *dynamodb.DynamoDB, table string, shard int, totalShards int, itemChan chan []map[string]*dynamodb.AttributeValue, lockChan chan byte) {
	counter := 1
	res, err := client.Scan(&dynamodb.ScanInput{
		ConsistentRead: aws.Bool(true),
		Select:         aws.String("ALL_ATTRIBUTES"),
		TableName:      &table,
		Segment:        aws.Int64(int64(shard)),
		TotalSegments:  aws.Int64(int64(totalShards)),
	})
	for len(res.LastEvaluatedKey) > 0 && err != nil {
		log.Printf("Shard %d pulled page %d from %s", shard, counter, table)
		counter++
		itemChan <- res.Items
		res, err = client.Scan(&dynamodb.ScanInput{
			ConsistentRead:    aws.Bool(true),
			Select:            aws.String("ALL_ATTRIBUTES"),
			TableName:         &table,
			ExclusiveStartKey: res.LastEvaluatedKey,
			Segment:           aws.Int64(int64(shard)),
			TotalSegments:     aws.Int64(int64(totalShards)),
		})
	}
	if err != nil {
		panic(err)
	}
	// one last page
	log.Printf("Shard %d finished pulling items from %s", shard, table)
	itemChan <- res.Items
	lockChan <- 1
}

func generateBatchWriteInput(table string, items []map[string]*dynamodb.AttributeValue) (*dynamodb.BatchWriteItemInput, error) {
	requestItems := make(map[string][]*dynamodb.WriteRequest)
	if len(items) > 25 {
		return nil, errors.New("Can not create batch write request > 25 items")
	}
	for i := range items {
		requestItems[table] = append(requestItems[table], &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: items[i],
			},
		})
	}
	return &dynamodb.BatchWriteItemInput{RequestItems: requestItems}, nil
}

func batchWriteItems(client *dynamodb.DynamoDB, table string, items []map[string]*dynamodb.AttributeValue, start int, end int, lockChan chan byte) {
	input, err := generateBatchWriteInput(table, items[start:end])
	res, batchErr := client.BatchWriteItem(input)
	// process unprocessed items
	unprocessed := res.UnprocessedItems
	for len(unprocessed) > 0 {
		log.Printf("%d unprocessed items. Processing...", len(unprocessed))
		res, err = client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
			RequestItems: unprocessed,
		})
		unprocessed = res.UnprocessedItems
		if err != nil {
			panic(err)
		}
	}
	if batchErr != nil {
		panic(err)
	}
	log.Printf("Stored items %d - %d in %s", start, end, table)
	lockChan <- 1
}

func pushItems(client *dynamodb.DynamoDB, destTable string, itemChan chan []map[string]*dynamodb.AttributeValue, waitChan chan byte) {
	// func pushItems(client *dynamodb.DynamoDB, destTable string, itemChan chan []map[string]*dynamodb.AttributeValue) {
	// batchsize max is 25
	batchSize := 15
	lockChan := make(chan byte)
	goroCount := 0
	lastBatch := 0

	for itemList := range itemChan {
		for i := range itemList {
			if i%batchSize == 0 && i > 0 {
				lastBatch = i
				goroCount++
				go batchWriteItems(client, destTable, itemList, i-batchSize, i, lockChan)
			} else if lastBatch+batchSize > len(itemList) {
				_, err := client.PutItem(&dynamodb.PutItemInput{
					TableName: &destTable,
					Item:      itemList[i],
				})
				log.Printf("Stored item %d in %s", i, destTable)
				if err != nil {
					panic(err)
				}
			}
		}
	}
	cleanUpThreads("PushItems", goroCount, lockChan)
	waitChan <- 1
}

func cleanUpThreads(source string, count int, channel chan byte) {
	log.Printf("Cleaning up %s threads. %d total threads", source, count)
	for i := 0; i < count; i++ {
		<-channel
		log.Printf("Closed %s thread %d of %d", source, i+1, count)
	}
}

func main() {
	sourceTable, present := os.LookupEnv("SOURCE_TABLE")
	if !present {
		panic("SOURCE_TABLE environment variable must be defined")
	}
	destTable, present := os.LookupEnv("DESTINATION_TABLE")
	if !present {
		panic("DESTINATION_TABLE environment variable must be defined")
	}
	log.Printf("Dynamodb migration from %s to %s", sourceTable, destTable)
	itemChan := make(chan []map[string]*dynamodb.AttributeValue)
	waitChan := make(chan byte, 2)
	ddb := dynamodb.New(session.New(&aws.Config{
		Region: aws.String("us-east-1"),
	}))
	log.Println("Pulling items from source table")
	go pullItems(ddb, sourceTable, itemChan, waitChan)
	go pushItems(ddb, destTable, itemChan, waitChan)
	<-waitChan
	<-waitChan
	// pullItems(ddb, sourceTable, itemChan)
	// pushItems(ddb, destTable, itemChan)
}
