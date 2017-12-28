package main

import (
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// Table provides the capability to read and write to tables
type Table struct {
	Name      string
	Client    dynamodbiface.DynamoDBAPI
	ItemCount int
	ItemChan  chan []map[string]*dynamodb.AttributeValue
}

// NewTable creates table struct and returns it
func NewTable(client dynamodbiface.DynamoDBAPI, name string) *Table {
	return &Table{
		Name:      name,
		Client:    client,
		ItemCount: 0,
		ItemChan:  make(chan []map[string]*dynamodb.AttributeValue),
	}
}

// UpdateTable submits the update to table capacity to dynamodb, and waits until\\
// the table update is complete
func (t *Table) UpdateTable(input *dynamodb.UpdateTableInput) {
	_, err := t.Client.UpdateTable(input)
	if err != nil {
		if strings.Contains(err.Error(), "will not change") {
			log.Printf("No updates to be made")
			return
		} else if strings.Contains(err.Error(), "is being updated") {
			log.Printf("Update already in progress")
		} else {
			panic(err)
		}
	}

	// parses describetable output
	getStatus := func(output *dynamodb.DescribeTableOutput) bool {
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

	// wait for table update to complete
	updateRes, err := t.Client.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: &t.Name,
	})

	for !getStatus(updateRes) {
		if err != nil {
			panic(err)
		}
		log.Printf("Waiting for table update to complete....")
		updateRes, err = t.Client.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: &t.Name,
		})
		time.Sleep(5 * time.Second)
	}
	log.Printf("Table update complete. Table in status %s", *updateRes.Table.TableStatus)
}

// IncreaseCapacity increases the read/write capacity of a table and all GlobalSecondaryIndexes
// 3000 read/writes is probably too many for most cases but should give runway for large tables
func (t *Table) IncreaseCapacity(readOrWrite string) *dynamodb.UpdateTableInput {
	// Get table throughput information
	res, err := t.Client.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: &t.Name,
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
		TableName: &t.Name,
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  res.Table.ProvisionedThroughput.ReadCapacityUnits,
			WriteCapacityUnits: res.Table.ProvisionedThroughput.WriteCapacityUnits,
		},
	}
	newSettings := dynamodb.UpdateTableInput{
		TableName: &t.Name,
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

	t.UpdateTable(&newSettings)

	return &originalSettings
}

// PullItems sends off async scans of dynamodb table and writes the items to a channel
func (t *Table) PullItems(waitChan chan byte) {
	// set table item count to 0, will be refreshed after items are pulled
	t.ItemCount = 0
	lockChan := make(chan byte)
	// dynamodb scanners
	shards := 5

	for i := 0; i < shards; i++ {
		go t.ScanTable(i, shards, lockChan)
	}
	cleanUpThreads("PullItems", shards, lockChan)
	close(t.ItemChan)
	log.Println("Item channel closed")
	waitChan <- 1
}

// ScanTable is executes a parallel scan on the dynamo database
// all items are appended to a channel.
func (t *Table) ScanTable(shard int, totalShards int, lockChan chan byte) {
	counter := 1

	res, err := t.Client.Scan(&dynamodb.ScanInput{
		ConsistentRead: aws.Bool(true),
		Select:         aws.String("ALL_ATTRIBUTES"),
		TableName:      &t.Name,
		Segment:        aws.Int64(int64(shard)),
		TotalSegments:  aws.Int64(int64(totalShards)),
	})

	for len(res.LastEvaluatedKey) > 0 && err == nil {
		log.Printf("Shard %d pulled page %d from %s", shard, counter, t.Name)
		counter++
		t.ItemCount = t.ItemCount + len(res.Items)
		makeBatches(t.ItemChan, res.Items)

		res, err = t.Client.Scan(&dynamodb.ScanInput{
			ConsistentRead:    aws.Bool(true),
			Select:            aws.String("ALL_ATTRIBUTES"),
			TableName:         &t.Name,
			ExclusiveStartKey: res.LastEvaluatedKey,
			Segment:           aws.Int64(int64(shard)),
			TotalSegments:     aws.Int64(int64(totalShards)),
		})
	}

	t.ItemCount = t.ItemCount + len(res.Items)
	makeBatches(t.ItemChan, res.Items)

	if err != nil {
		panic(err)
	}

	log.Printf("Shard %d finished pulling items from %s", shard, t.Name)
	lockChan <- 1
}

// PushItems creates many goroutines to write items to DynamoDB
// Items are written through the BatchWriteItem method and unprocessed items are retried three times
// the number of concurrent threads is limited by the maxThreads variable
func (t *Table) PushItems(itemChan chan []map[string]*dynamodb.AttributeValue, waitChan chan byte) {
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
		go t.BatchWriteItems(itemList, concurrentThreads, lockChan, unprocessedItems)
	}
	cleanUpThreads("PushItems", goroCount, lockChan)
	close(unprocessedItems)
	log.Printf("Cleaning up %d unprocessed items", len(unprocessedItems))
	counter = 0
	for i := range unprocessedItems {
		counter++
		log.Printf("Storing unprocessed item %d", counter)
		_, err := t.Client.PutItem(&dynamodb.PutItemInput{
			TableName: &t.Name,
			Item:      i,
		})
		if err != nil {
			log.Printf("%+v", err)
		}
	}
	waitChan <- 1
}

// BatchWriteItems executes an aws batchWriteItems command and handles the unprocessed items
// Unprocessed items are retried 3 times and then appended to a channel to be processed later
func (t *Table) BatchWriteItems(items []map[string]*dynamodb.AttributeValue, concurrentThreads chan bool, lockChan chan byte, unprocessedItems chan map[string]*dynamodb.AttributeValue) {
	input, _ := generateBatchWriteInput(t.Name, items)
	res, batchErr := t.Client.BatchWriteItem(input)
	if batchErr != nil {
		panic(batchErr)
	}
	// process unprocessed items
	// retries 3 times
	unprocessed := res.UnprocessedItems
	retries := 0
	for len(unprocessed) > 0 && retries < 3 {
		log.Printf("%d unprocessed items. Processing...", len(unprocessed))
		res, err := t.Client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
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
		for u := range unprocessed[t.Name] {
			log.Printf("Length of unprocessed items channel: %d", len(unprocessedItems))
			unprocessedItems <- unprocessed[t.Name][u].PutRequest.Item
		}
	}
	// free up room for another thread
	concurrentThreads <- true
	// for syncing
	lockChan <- 1
}
