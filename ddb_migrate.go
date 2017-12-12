package main

import (
	"errors"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Checks if DescribeTableOutput is active or not
func getStatus(output *dynamodb.DescribeTableOutput) bool {
	active := true
	if *output.Table.TableStatus != "ACTIVE" {
		active = false
		return active
	}
	for i := range output.Table.GlobalSecondaryIndexes {
		if *output.Table.GlobalSecondaryIndexes[i].IndexStatus != "ACTIVE" {
			active = false
			return active
		}
	}
	return active
}

func updateCapacity(table string, readOrWrite string, client *dynamodb.DynamoDB) *dynamodb.UpdateTableInput {
	// Get table throughput information
	res, err := client.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: &table,
	})
	if err != nil {
		panic(err)
	}
	var readCapacity *int64
	var writeCapacity *int64

	if readOrWrite == "write" {
		readCapacity = res.Table.ProvisionedThroughput.ReadCapacityUnits
		writeCapacity = aws.Int64(3000)
	} else {
		readCapacity = aws.Int64(3000)
		writeCapacity = res.Table.ProvisionedThroughput.WriteCapacityUnits
	}
	originalSettings := dynamodb.UpdateTableInput{
		TableName: &table,
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  res.Table.ProvisionedThroughput.ReadCapacityUnits,
			WriteCapacityUnits: res.Table.ProvisionedThroughput.WriteCapacityUnits,
		},
	}
	newSettings := dynamodb.UpdateTableInput{
		TableName: &table,
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			WriteCapacityUnits: writeCapacity,
			ReadCapacityUnits:  readCapacity,
		},
	}
	// Collect global secondary indexes and increase capacity as well
	for i := range res.Table.GlobalSecondaryIndexes {
		if readOrWrite == "write" {
			readCapacity = res.Table.ProvisionedThroughput.ReadCapacityUnits
			writeCapacity = aws.Int64(3000)
		} else {
			readCapacity = aws.Int64(3000)
			writeCapacity = res.Table.ProvisionedThroughput.WriteCapacityUnits
		}
		if res.Table.GlobalSecondaryIndexes[i].ProvisionedThroughput.WriteCapacityUnits != writeCapacity || res.Table.GlobalSecondaryIndexes[i].ProvisionedThroughput.ReadCapacityUnits != readCapacity {
			newSettings.GlobalSecondaryIndexUpdates = append(newSettings.GlobalSecondaryIndexUpdates, &dynamodb.GlobalSecondaryIndexUpdate{
				Update: &dynamodb.UpdateGlobalSecondaryIndexAction{
					IndexName: res.Table.GlobalSecondaryIndexes[i].IndexName,
					ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
						WriteCapacityUnits: writeCapacity,
						ReadCapacityUnits:  readCapacity,
					},
				},
			})
			originalSettings.GlobalSecondaryIndexUpdates = append(newSettings.GlobalSecondaryIndexUpdates, &dynamodb.GlobalSecondaryIndexUpdate{
				Update: &dynamodb.UpdateGlobalSecondaryIndexAction{
					IndexName: res.Table.GlobalSecondaryIndexes[i].IndexName,
					ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
						WriteCapacityUnits: res.Table.GlobalSecondaryIndexes[i].ProvisionedThroughput.WriteCapacityUnits,
						ReadCapacityUnits:  res.Table.GlobalSecondaryIndexes[i].ProvisionedThroughput.ReadCapacityUnits,
					},
				},
			})
		}
	}

	_, err = client.UpdateTable(&newSettings)
	if err != nil {
		if strings.Contains(err.Error(), "will not change") {
			log.Printf("No updates to be made")
		} else if strings.Contains(err.Error(), "is being updated") {
			log.Printf("Update already in progress")
		} else {
			panic(err)
		}
	}

	updateRes, err := client.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: &table,
	})

	// wait for table update to complete
	for !getStatus(updateRes) {
		if err != nil {
			panic(err)
		}
		log.Printf("Waiting for table update to complete....")
		updateRes, err = client.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: &table,
		})
		time.Sleep(5 * time.Second)
	}
	log.Printf("Table update complete. Table in status %s", *updateRes.Table.TableStatus)

	return &originalSettings
}

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

// scanTable is executes a parallel scan on the dynamo database
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

	for len(res.LastEvaluatedKey) > 0 && err == nil {
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

func batchWriteItems(client *dynamodb.DynamoDB, table string, items []map[string]*dynamodb.AttributeValue, concurrentThreads chan bool, lockChan chan byte, unprocessedItems chan map[string]*dynamodb.AttributeValue) {
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
		// try not to overload tables
		time.Sleep(1 * time.Second)
	}
	if retries == 3 && len(unprocessed) > 0 {
		log.Printf("Unable to process %d items", len(unprocessed))
		for u := range unprocessed[table] {
			log.Printf("Length of unprocessed items channel: %d", len(unprocessedItems))
			unprocessedItems <- unprocessed[table][u].PutRequest.Item
		}
	}
	// free up room for another thread
	concurrentThreads <- true
	// for syncing
	lockChan <- 1
}

func pushItems(client *dynamodb.DynamoDB, destTable string, itemChan chan []map[string]*dynamodb.AttributeValue, waitChan chan byte) {
	lockChan := make(chan byte)
	goroCount := 0
	counter := 0
	unprocessedItems := make(chan map[string]*dynamodb.AttributeValue, 100000)

	// limit concurrency through use of buffered channel
	maxThreads := 30
	concurrentThreads := make(chan bool, maxThreads)

	for i := 0; i < maxThreads; i++ {
		concurrentThreads <- true
	}

	for itemList := range itemChan {
		goroCount++
		counter = counter + len(itemList)
		<-concurrentThreads
		log.Printf("Thread %d/%d Dispatched items %d-%d for processing", maxThreads-len(concurrentThreads), maxThreads, counter-len(itemList), counter)
		go batchWriteItems(client, destTable, itemList, concurrentThreads, lockChan, unprocessedItems)
	}
	cleanUpThreads("PushItems", goroCount, lockChan)
	close(unprocessedItems)
	log.Printf("Cleaning up %d unprocessed items", len(unprocessedItems))
	counter = 0
	for i := range unprocessedItems {
		counter++
		log.Printf("Storing unprocessed item %d", counter)
		_, err := client.PutItem(&dynamodb.PutItemInput{
			TableName: &destTable,
			Item:      i,
		})
		if err != nil {
			log.Printf("%+v", err)
		}
	}
	waitChan <- 1
}

func cleanUpThreads(source string, count int, channel chan byte) {
	log.Printf("Cleaning up %d threads for %s", count, source)
	for i := 0; i < count; i++ {
		log.Printf("Cleaned up thread %d of %d for %s", i, count, source)
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
	log.Printf("Increasing table %s write capacity to 3000 for table and all GSIs", destTable)
	destOrigSettings := updateCapacity(destTable, "write", ddb)
	log.Printf("Increasing table %s read capacity to 3000 for table and all GSIs", sourceTable)
	srcOrigSettings := updateCapacity(sourceTable, "read", ddb)
	log.Println("Pulling items from source table")
	go pullItems(ddb, sourceTable, itemChan, waitChan)
	go pushItems(ddb, destTable, itemChan, waitChan)
	<-waitChan
	<-waitChan
	log.Printf("Knocking table %s write capacity back down to original settings", destTable)
	ddb.UpdateTable(destOrigSettings)
	log.Printf("Knocking table %s read capacity back down to original settings", sourceTable)
	ddb.UpdateTable(srcOrigSettings)

}
