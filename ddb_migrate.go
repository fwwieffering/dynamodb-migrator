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

func batchWriteItems(client *dynamodb.DynamoDB, table string, items []map[string]*dynamodb.AttributeValue, start int, end int, lockChan chan byte, unprocessedItems chan map[string]*dynamodb.AttributeValue) {
	input, _ := generateBatchWriteInput(table, items[start:end])
	res, batchErr := client.BatchWriteItem(input)
	// process unprocessed items
	// retries 5 times
	unprocessed := res.UnprocessedItems
	// skip retries for now, just gather them all up and PutItem at the end
	// retries := 0
	// for len(unprocessed) > 0 && retries < 5 {
	// 	log.Printf("%d unprocessed items. Processing...", len(unprocessed))
	// 	res, err = client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
	// 		RequestItems: unprocessed,
	// 	})
	// 	unprocessed = res.UnprocessedItems
	// 	retries++
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }
	// if retries == 3 && len(unprocessed) > 0 {
	if len(unprocessed) > 0 {
		log.Printf("Unable to process %d items", len(unprocessed))
		for u := range unprocessed[table] {
			unprocessedItems <- unprocessed[table][u].PutRequest.Item
		}
	}
	if batchErr != nil {
		panic(batchErr)
	}
	lockChan <- 1
}

func pushItems(client *dynamodb.DynamoDB, destTable string, itemChan chan []map[string]*dynamodb.AttributeValue, waitChan chan byte) {
	// batchsize max is 25
	batchSize := 15
	lockChan := make(chan byte)
	goroCount := 0
	lastBatch := 0
	counter := 0
	unprocessedItems := make(chan map[string]*dynamodb.AttributeValue)

	for itemList := range itemChan {
		for i := range itemList {
			counter++
			if i%batchSize == 0 && i > 0 {
				lastBatch = i
				goroCount++
				log.Printf("Dispatched items %d-%d for processing", counter-batchSize, counter)
				go batchWriteItems(client, destTable, itemList, i-batchSize, i, lockChan, unprocessedItems)
			} else if lastBatch+batchSize > len(itemList) {
				_, err := client.PutItem(&dynamodb.PutItemInput{
					TableName: &destTable,
					Item:      itemList[i],
				})
				log.Printf("Stored item %d in %s", counter, destTable)
				if err != nil {
					panic(err)
				}
			}
		}
	}
	cleanUpThreads("PushItems", goroCount, lockChan)
	close(unprocessedItems)
	log.Printf("Cleaning up %d unprocessed items", len(unprocessedItems))
	for i := range unprocessedItems {
		client.PutItem(&dynamodb.PutItemInput{
			TableName: &destTable,
			Item:      i,
		})
	}
	waitChan <- 1
}

func cleanUpThreads(source string, count int, channel chan byte) {
	for i := 0; i < count; i++ {
		<-channel
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
}
