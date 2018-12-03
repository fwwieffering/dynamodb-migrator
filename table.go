package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	log "github.com/sirupsen/logrus"
)

// Table provides the capability to read and write to tables
type Table struct {
	Name      string
	Client    dynamodbiface.DynamoDBAPI
	ItemCount *ItemCount
	ItemChan  chan []map[string]*dynamodb.AttributeValue
}

// NewTable creates table struct and returns it
func NewTable(client dynamodbiface.DynamoDBAPI, name string) *Table {
	return &Table{
		Name:      name,
		Client:    client,
		ItemCount: NewItemCount(),
		ItemChan:  make(chan []map[string]*dynamodb.AttributeValue),
	}
}

// PullItems sends off async scans of dynamodb table and writes the items to a channel
func (t *Table) PullItems(waitChan chan byte) {
	// set table item count to 0, will be refreshed after items are pulled
	t.ItemCount.Set(0)
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

	res, err := handleScanProvisionedThroughput(t.Client, &dynamodb.ScanInput{
		ConsistentRead: aws.Bool(true),
		Select:         aws.String("ALL_ATTRIBUTES"),
		TableName:      &t.Name,
		Segment:        aws.Int64(int64(shard)),
		TotalSegments:  aws.Int64(int64(totalShards)),
	})

	for len(res.LastEvaluatedKey) > 0 && err == nil {
		log.Infoln(fmt.Sprintf("Shard %d pulled page %d from %s", shard, counter, t.Name))
		counter++
		t.ItemCount.Add(len(res.Items))
		makeBatches(t.ItemChan, res.Items)

		res, err = handleScanProvisionedThroughput(t.Client, &dynamodb.ScanInput{
			ConsistentRead:    aws.Bool(true),
			Select:            aws.String("ALL_ATTRIBUTES"),
			TableName:         &t.Name,
			ExclusiveStartKey: res.LastEvaluatedKey,
			Segment:           aws.Int64(int64(shard)),
			TotalSegments:     aws.Int64(int64(totalShards)),
		})
	}
	if err != nil {
		panic(err)
	}

	t.ItemCount.Add(len(res.Items))
	makeBatches(t.ItemChan, res.Items)

	log.Infoln(fmt.Sprintf("Shard %d finished pulling items from %s", shard, t.Name))
	lockChan <- 1
}

// PushItems creates many goroutines to write items to DynamoDB
// Items are written through the BatchWriteItem method and unprocessed items are retried three times
// the number of concurrent threads is limited by the maxThreads variable
func (t *Table) PushItems(itemChan chan []map[string]*dynamodb.AttributeValue, waitChan chan byte) {
	lockChan := make(chan byte)
	// WARNING: more than 100000 unprocessed items causes error
	// TODO: mutex / slice instead of chan
	unprocessedItems := make(chan map[string]*dynamodb.AttributeValue, 100000)

	// limit concurrency through use of buffered channel
	maxThreads := 5

	itemCounter := NewItemCount()
	for i := 0; i < maxThreads; i++ {
		go t.BatchWriteItems(itemChan, lockChan, itemCounter, fmt.Sprintf("%d/%d", i+1, maxThreads), unprocessedItems)
	}
	cleanUpThreads("PushItems", maxThreads, lockChan)

	close(unprocessedItems)
	log.Infoln(fmt.Sprintf("Cleaning up %d unprocessed items", len(unprocessedItems)))
	counter := 0
	for i := range unprocessedItems {
		counter++
		log.Infoln(fmt.Sprintf("Storing unprocessed item %d", counter))
		_, err := t.Client.PutItem(&dynamodb.PutItemInput{
			TableName: &t.Name,
			Item:      i,
		})
		if err != nil {
			log.Errorln(fmt.Sprintf("%+v", err))
		}
	}
	waitChan <- 1
}

// BatchWriteItems executes an aws batchWriteItems command and handles the unprocessed items
// Unprocessed items are retried 3 times and then appended to a channel to be processed later
func (t *Table) BatchWriteItems(itemChan chan []map[string]*dynamodb.AttributeValue, lockChan chan byte, itemCount *ItemCount, threadID string, unprocessedItems chan map[string]*dynamodb.AttributeValue) {
	for itemList := range itemChan {
		// reporting
		itemCount.Lock.Lock()
		itemCount.Count = itemCount.Count + len(itemList)
		beginningRange := itemCount.Count - len(itemList)
		endRange := itemCount.Count
		itemCount.Lock.Unlock()
		log.Infof(fmt.Sprintf("Thread %s Dispatched items %d-%d for processing", threadID, beginningRange, endRange))

		// do write
		input, _ := generateBatchWriteInput(t.Name, itemList)
		res, batchErr := handleWriteProvisionedThroughput(t.Client, input)
		if batchErr != nil {
			panic(batchErr)
		}
		// process unprocessed items
		// retries 3 times
		unprocessed := res.UnprocessedItems
		retries := 0
		for len(unprocessed) > 0 && retries < 3 {
			log.Infoln(fmt.Sprintf("Thread %s has %d unprocessed items in items %d-%d Processing...", threadID, len(unprocessed), beginningRange, endRange))
			res, err := handleWriteProvisionedThroughput(t.Client, &dynamodb.BatchWriteItemInput{
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
			log.Errorln(fmt.Sprintf("Thread %s unable to process %d items in items %d-%d. Writing them to the cleanup channel", threadID, len(unprocessed), beginningRange, endRange))
			for u := range unprocessed[t.Name] {
				unprocessedItems <- unprocessed[t.Name][u].PutRequest.Item
				log.Infoln(fmt.Sprintf("Length of unprocessed items channel: %d", len(unprocessedItems)))
			}
		}
	}
	// for syncing
	lockChan <- 1
}
