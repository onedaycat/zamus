package batchdynamo

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"golang.org/x/sync/errgroup"
)

func LargeBatchWrite(ctx context.Context, db *dynamodb.DynamoDB, tableName string, size int, reqs []*dynamodb.WriteRequest) error {
	var batchReqs []*dynamodb.WriteRequest
	curSize := 0
	maxSize := len(reqs)
	inputs := make([]*dynamodb.BatchWriteItemInput, 0, (maxSize/size)+1)

	for i := 0; i < maxSize; i += size {
		curSize = i + size
		if curSize > maxSize {
			batchReqs = reqs[i:maxSize]
		}
		batchReqs = reqs[i:curSize]

		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				tableName: batchReqs,
			},
		}

		inputs = append(inputs, input)
	}

	wg := &errgroup.Group{}
	for _, in := range inputs {
		in := in
		wg.Go(func() error {
			_, err := db.BatchWriteItemWithContext(ctx, in)
			return err
		})
	}

	return wg.Wait()
}
