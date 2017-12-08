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

// makeBatches creates smaller arrays out of large arrays and appends to a channel
func makeBatches(itemChan chan []map[string]*dynamodb.AttributeValue, itemList []map[string]*dynamodb.AttributeValue) {
	batchSize := 15
	lastBatch := 0
	for item := range itemList {
		if item%batchSize == 0 && item > 0 {
			itemChan <- itemList[item-batchSize : item]
			lastBatch = item
		}
	}
	if lastBatch != len(itemList) {
		itemChan <- itemList[lastBatch:]
	}
}

func scanTable(client *dynamodb.DynamoDB, table string, shard int, totalShards int, itemChan chan []map[string]*dynamodb.AttributeValue, lockChan chan byte) {
	counter := 1
	itemCount := 0

	res, err := client.Scan(&dynamodb.ScanInput{
		ConsistentRead: aws.Bool(true),
		Select:         aws.String("ALL_ATTRIBUTES"),
		TableName:      &table,
		Segment:        aws.Int64(int64(shard)),
		TotalSegments:  aws.Int64(int64(totalShards)),
	})
	itemCount = itemCount + len(res.Items)
	makeBatches(itemChan, res.Items)

	for len(res.LastEvaluatedKey) > 0 && err != nil {
		log.Printf("Shard %d pulled page %d from %s", shard, counter, table)
		counter++
		itemCount = itemCount + len(res.Items)
		makeBatches(itemChan, res.Items)

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
	log.Printf("Shard %d finished pulled %d items from %s", shard, itemCount, table)
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

func batchWriteItems(client *dynamodb.DynamoDB, table string, items []map[string]*dynamodb.AttributeValue, lockChan chan byte, unprocessedItems chan map[string]*dynamodb.AttributeValue) {
	input, err := generateBatchWriteInput(table, items)
	if err != nil {
		panic(err)
	}
	res, batchErr := client.BatchWriteItem(input)
	if batchErr != nil {
		panic(batchErr)
	}
	// process unprocessed items
	// retries 3 times
	unprocessed := res.UnprocessedItems
	// skip retries for now, just gather them all up and PutItem at the end
	retries := 0
	for len(unprocessed) > 0 && retries < 3 {
		log.Printf("%d unprocessed items. Processing...", len(unprocessed))
		res, err = client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
			RequestItems: unprocessed,
		})
		unprocessed = res.UnprocessedItems
		retries++
		if err != nil {
			panic(err)
		}
	}
	if retries == 3 && len(unprocessed) > 0 {
		// if len(unprocessed) > 0 {
		log.Printf("Unable to process %d items", len(unprocessed))
		for u := range unprocessed[table] {
			unprocessedItems <- unprocessed[table][u].PutRequest.Item
		}
	}
	lockChan <- 1
}

func pushItems(client *dynamodb.DynamoDB, destTable string, itemChan chan []map[string]*dynamodb.AttributeValue, waitChan chan byte) {
	// batchsize max is 25
	lockChan := make(chan byte)
	goroCount := 0
	counter := 0
	unprocessedItems := make(chan map[string]*dynamodb.AttributeValue)

	for itemList := range itemChan {
		counter = counter + len(itemList)
		goroCount++
		log.Printf("Dispatched items %d-%d for processing", counter-len(itemList), counter)
		go batchWriteItems(client, destTable, itemList, lockChan, unprocessedItems)
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
