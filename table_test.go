package main

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type mockDynamo struct {
	dynamodbiface.DynamoDBAPI
	// to not have dupes we will have a map of event-id -> map of items
	items              map[string]map[string]*dynamodb.AttributeValue
	itemsMutex         sync.RWMutex
	scanTableResp      []*dynamodb.ScanOutput
	describeTableResp  []*dynamodb.DescribeTableOutput
	putItemResp        []*dynamodb.PutItemOutput
	batchWriteItemResp []*dynamodb.BatchWriteItemOutput
	err                []error
}

func (m *mockDynamo) getErr() error {
	var resp error
	if m.err != nil {
		resp = m.err[0]
		if len(m.err) > 1 {
			m.err = m.err[1:]
		}
	}
	return resp
}

func (m *mockDynamo) DescribeTable(*dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
	resp := m.describeTableResp[0]
	if len(m.describeTableResp) > 1 {
		m.describeTableResp = m.describeTableResp[1:]
	}
	return resp, m.getErr()
}

func (m *mockDynamo) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	if m.items == nil {
		m.items = make(map[string]map[string]*dynamodb.AttributeValue)
	}
	m.itemsMutex.Lock()
	m.items[*input.Item["event-id"].S] = input.Item
	m.itemsMutex.Unlock()

	resp := m.putItemResp[0]
	if len(m.putItemResp) > 1 {
		m.putItemResp = m.putItemResp[1:]
	}
	return resp, m.getErr()
}

func (m *mockDynamo) Scan(input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	resp := m.scanTableResp[0]
	if len(m.scanTableResp) > 1 {
		m.scanTableResp = m.scanTableResp[1:]
	}
	return resp, m.getErr()
}

func (m *mockDynamo) BatchWriteItem(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
	if m.items == nil {
		m.items = make(map[string]map[string]*dynamodb.AttributeValue)
	}
	resp := m.batchWriteItemResp[0]
	for _, value := range input.RequestItems {
		for req := range value {
			process := true
			// check if item is in unprocessed item list
			for _, v := range resp.UnprocessedItems {
				for j := range v {
					if *v[j].PutRequest.Item["event-id"].S == *value[req].PutRequest.Item["event-id"].S {
						process = false
					}
				}
			}
			if process {
				m.itemsMutex.Lock()
				m.items[*value[req].PutRequest.Item["event-id"].S] = value[req].PutRequest.Item
				m.itemsMutex.Unlock()
			}
		}
	}
	if len(m.batchWriteItemResp) > 1 {
		m.batchWriteItemResp = m.batchWriteItemResp[1:]
	}
	return resp, m.getErr()
}

func TestNewTable(t *testing.T) {
	tab := NewTable(&mockDynamo{}, "testTable")
	if tab.Name != "testTable" {
		t.Fatal("Name is not set correctly")
	}
}

func TestScan(t *testing.T) {
	lockChan := make(chan byte, 1)
	tab := NewTable(&mockDynamo{
		scanTableResp: []*dynamodb.ScanOutput{
			{
				LastEvaluatedKey: map[string]*dynamodb.AttributeValue{
					"something": &dynamodb.AttributeValue{},
				},
				Items: make([]map[string]*dynamodb.AttributeValue, 100),
			},
			{
				LastEvaluatedKey: make(map[string]*dynamodb.AttributeValue, 0),
				Items:            make([]map[string]*dynamodb.AttributeValue, 10),
			},
		},
	}, "face")
	// have to make buffered chan or write will block
	tab.ItemChan = make(chan []map[string]*dynamodb.AttributeValue, 10)
	tab.ScanTable(1, 1, lockChan)
	<-lockChan
	close(tab.ItemChan)
	if tab.ItemCount != 110 {
		t.Errorf("Item count should be 20 after scan table call")
	}
	if len(tab.ItemChan) != 8 {
		t.Errorf("110 items should result in 8 channel entries")
	}
	counter := 0
	for len(tab.ItemChan) > 0 {
		batch := <-tab.ItemChan
		counter = counter + len(batch)
	}
	if counter != 110 {
		t.Errorf("Counter: %d. Should be 110, either missing items or has extras", counter)
	}
}

func TestScanPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()
	tab := NewTable(&mockDynamo{
		err: []error{errors.New("some dang error")},
		scanTableResp: []*dynamodb.ScanOutput{
			{
				LastEvaluatedKey: make(map[string]*dynamodb.AttributeValue, 0),
			},
		},
	}, "face")
	lockChan := make(chan byte, 1)
	tab.ScanTable(1, 1, lockChan)
}

func TestPullItems(t *testing.T) {
	tab := NewTable(&mockDynamo{
		scanTableResp: []*dynamodb.ScanOutput{
			{
				LastEvaluatedKey: make(map[string]*dynamodb.AttributeValue, 0),
				Items:            make([]map[string]*dynamodb.AttributeValue, 100),
			},
		},
	}, "face")
	// have to make buffered chan or write will block
	tab.ItemChan = make(chan []map[string]*dynamodb.AttributeValue, 50)
	lockChan := make(chan byte, 1)
	tab.PullItems(lockChan)
	<-lockChan
	if tab.ItemCount != 500 {
		t.Errorf("item count should be 500. Is: %d", tab.ItemCount)
	}
}

func TestBatchWriteItems(t *testing.T) {
	itemList := make([]map[string]*dynamodb.AttributeValue, 0, 100)
	for i := 0; i < 100; i++ {
		itemList = append(itemList, map[string]*dynamodb.AttributeValue{
			"event-id": &dynamodb.AttributeValue{S: aws.String(fmt.Sprintf("item-%d", i))},
		})
	}
	// Items item-0 and item-1 should never be processed
	// 4 responses of unprocessed items triggers unprocessed items on first BatchWriteItem call
	// afterwards, no unprocessed items
	tab := NewTable(&mockDynamo{
		batchWriteItemResp: []*dynamodb.BatchWriteItemOutput{
			{
				UnprocessedItems: map[string][]*dynamodb.WriteRequest{
					"face": []*dynamodb.WriteRequest{
						{
							PutRequest: &dynamodb.PutRequest{
								Item: map[string]*dynamodb.AttributeValue{
									"event-id": &dynamodb.AttributeValue{S: aws.String("item-0")},
								},
							},
						},
						{
							PutRequest: &dynamodb.PutRequest{
								Item: map[string]*dynamodb.AttributeValue{
									"event-id": &dynamodb.AttributeValue{S: aws.String("item-1")},
								},
							},
						},
					},
				},
			},
			{
				UnprocessedItems: map[string][]*dynamodb.WriteRequest{
					"face": []*dynamodb.WriteRequest{
						{
							PutRequest: &dynamodb.PutRequest{
								Item: map[string]*dynamodb.AttributeValue{
									"event-id": &dynamodb.AttributeValue{S: aws.String("item-0")},
								},
							},
						},
						{
							PutRequest: &dynamodb.PutRequest{
								Item: map[string]*dynamodb.AttributeValue{
									"event-id": &dynamodb.AttributeValue{S: aws.String("item-1")},
								},
							},
						},
					},
				},
			},
			{
				UnprocessedItems: map[string][]*dynamodb.WriteRequest{
					"face": []*dynamodb.WriteRequest{
						{
							PutRequest: &dynamodb.PutRequest{
								Item: map[string]*dynamodb.AttributeValue{
									"event-id": &dynamodb.AttributeValue{S: aws.String("item-0")},
								},
							},
						},
						{
							PutRequest: &dynamodb.PutRequest{
								Item: map[string]*dynamodb.AttributeValue{
									"event-id": &dynamodb.AttributeValue{S: aws.String("item-1")},
								},
							},
						},
					},
				},
			},
			{
				UnprocessedItems: map[string][]*dynamodb.WriteRequest{
					"face": []*dynamodb.WriteRequest{
						{
							PutRequest: &dynamodb.PutRequest{
								Item: map[string]*dynamodb.AttributeValue{
									"event-id": &dynamodb.AttributeValue{S: aws.String("item-0")},
								},
							},
						},
						{
							PutRequest: &dynamodb.PutRequest{
								Item: map[string]*dynamodb.AttributeValue{
									"event-id": &dynamodb.AttributeValue{S: aws.String("item-1")},
								},
							},
						},
					},
				},
			},
			{
				UnprocessedItems: make(map[string][]*dynamodb.WriteRequest),
			},
		},
	}, "face")
	itemChan := make(chan []map[string]*dynamodb.AttributeValue, 10)
	makeBatches(itemChan, itemList)
	threadChan := make(chan bool, 1)
	lockChan := make(chan byte, 1)
	unprocessedItemsChan := make(chan map[string]*dynamodb.AttributeValue, 100)
	for len(itemChan) > 0 {
		list := <-itemChan
		tab.BatchWriteItems(list, threadChan, lockChan, unprocessedItemsChan)
		<-lockChan
		<-threadChan
	}
	if len(unprocessedItemsChan) > 2 {
		t.Log("Unprocessed items:")
		for len(unprocessedItemsChan) > 2 {
			item := <-unprocessedItemsChan
			t.Logf("%v", *item["event-id"].S)
		}
		t.Errorf("Unprocessed items channel should only have two items, item-0 and item-1")
	}
}

func TestBatchWritePanic1(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()
	threadChan := make(chan bool, 1)
	lockChan := make(chan byte, 1)
	unprocessedItemsChan := make(chan map[string]*dynamodb.AttributeValue, 100)
	items := []map[string]*dynamodb.AttributeValue{
		{
			"event-id": &dynamodb.AttributeValue{S: aws.String("some dang thing")},
		},
	}
	tab := NewTable(&mockDynamo{
		err: []error{errors.New("some dang error")},
		batchWriteItemResp: []*dynamodb.BatchWriteItemOutput{
			{
				UnprocessedItems: make(map[string][]*dynamodb.WriteRequest, 0),
			},
		},
	}, "face")

	tab.BatchWriteItems(items, threadChan, lockChan, unprocessedItemsChan)
}

func TestBatchWritePanic2(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()
	threadChan := make(chan bool, 1)
	lockChan := make(chan byte, 1)
	unprocessedItemsChan := make(chan map[string]*dynamodb.AttributeValue, 100)
	items := []map[string]*dynamodb.AttributeValue{
		{
			"event-id": &dynamodb.AttributeValue{S: aws.String("some dang thing")},
		},
	}
	tab := NewTable(&mockDynamo{
		err: []error{nil, errors.New("some dang error")},
		batchWriteItemResp: []*dynamodb.BatchWriteItemOutput{
			{
				UnprocessedItems: map[string][]*dynamodb.WriteRequest{
					"face": []*dynamodb.WriteRequest{
						{
							PutRequest: &dynamodb.PutRequest{
								Item: map[string]*dynamodb.AttributeValue{
									"event-id": &dynamodb.AttributeValue{S: aws.String("some dang thing")},
								},
							},
						},
					},
				},
			},
		},
	}, "face")

	tab.BatchWriteItems(items, threadChan, lockChan, unprocessedItemsChan)
}

func TestPushItems(t *testing.T) {
	itemList := make([]map[string]*dynamodb.AttributeValue, 0, 100)
	for i := 0; i < 100; i++ {
		itemList = append(itemList, map[string]*dynamodb.AttributeValue{
			"event-id": &dynamodb.AttributeValue{S: aws.String(fmt.Sprintf("item-%d", i))},
		})
	}
	tab := NewTable(&mockDynamo{
		batchWriteItemResp: []*dynamodb.BatchWriteItemOutput{
			{
				UnprocessedItems: map[string][]*dynamodb.WriteRequest{
					"face": []*dynamodb.WriteRequest{
						{
							PutRequest: &dynamodb.PutRequest{
								Item: map[string]*dynamodb.AttributeValue{
									"event-id": &dynamodb.AttributeValue{S: aws.String("some dang thing")},
								},
							},
						},
					},
				},
			},
		},
		putItemResp: []*dynamodb.PutItemOutput{
			&dynamodb.PutItemOutput{},
		},
	}, "face")
	waitChan := make(chan byte, 1)
	itemChan := make(chan []map[string]*dynamodb.AttributeValue)
	go tab.PushItems(itemChan, waitChan)
	makeBatches(itemChan, itemList)
	close(itemChan)
	<-waitChan
}
