package csv

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/comfforts/logger"

	"github.com/comfforts/localstorage/pkg/models"
)

const TEST_DIR = "data"

func TestPrincipals(t *testing.T) {
	logger := logger.NewTestAppLogger(TEST_DIR)

	file, err := os.Open("data/Principals.csv")
	require.NoError(t, err)

	csvFiler, err := NewCSVFiler(file, logger)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resCh := make(chan []string)
	errCh := make(chan error)

	go csvFiler.ReadCSVFile(ctx, resCh, errCh)

	errs := map[string]int{}
	res := map[int]*models.Entity{}
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("TestPrincipals: context done, returning. resCount: %d, errCnt: %v\n", len(res), errs)
			return
		case r, ok := <-resCh:
			if !ok {
				fmt.Printf("TestPrincipals: resultstream closed, returning. resCount: %d, errCnt: %v\n", len(res), errs)
				return
			} else {
				if r != nil {
					entity := models.MapToEntity(res, r)
					fmt.Printf("TestPrincipals: entity: %v\n", entity)
				}
			}
		case err, ok := <-errCh:
			if !ok {
				fmt.Printf("TestPrincipals: error stream closed, returning resCount: %d, errCnt: %v\n", len(res), errs)
				return
			} else {
				if err != nil {
					fmt.Printf("TestPrincipals - error: %v\n", err)
					errs[err.Error()]++
				}
			}
		}
	}
}

func TestAgents(t *testing.T) {
	logger := logger.NewTestAppLogger(TEST_DIR)

	file, err := os.Open("data/Agents.csv")
	require.NoError(t, err)

	csvFiler, err := NewCSVFiler(file, logger)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resCh := make(chan []string)
	errCh := make(chan error)

	go csvFiler.ReadCSVFile(ctx, resCh, errCh)

	errs := map[string]int{}
	res := map[int]*models.Entity{}
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("TestAgents: context done, returning. resCount: %d, errCnt: %v\n", len(res), errs)
			return
		case r, ok := <-resCh:
			if !ok {
				fmt.Printf("TestAgents: resultstream closed, returning. resCount: %d, errCnt: %v\n", len(res), errs)
				return
			} else {
				if r != nil {
					entity := models.MapToEntity(res, r)
					fmt.Printf("TestAgents: entity: %v\n", entity)
				}
			}
		case err, ok := <-errCh:
			if !ok {
				fmt.Printf("TestAgents: error stream closed, returning resCount: %d, errCnt: %v\n", len(res), errs)
				return
			} else {
				if err != nil {
					fmt.Printf("TestAgents, error: %v\n", err)
					errs[err.Error()]++
				}
			}
		}
	}
}

func TestFilings(t *testing.T) {
	logger := logger.NewTestAppLogger(TEST_DIR)

	file, err := os.Open("data/Filings.csv")
	require.NoError(t, err)

	csvFiler, err := NewCSVFiler(file, logger)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resCh := make(chan []string)
	errCh := make(chan error)

	go csvFiler.ReadCSVFile(ctx, resCh, errCh)

	errs := map[string]int{}
	res := map[int][]*models.Entity{}
	var resCount int
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("TestFilings: context done, returning. recordCount: %d, resCount: %d, errCnt: %v\n", resCount, len(res), errs)
			return
		case r, ok := <-resCh:
			if !ok {
				fmt.Printf("TestFilings: resultstream closed, returning. resCount: %d, resCount: %d, errCnt: %v\n", resCount, len(res), errs)
				return
			} else {
				if r != nil {
					entity, entErrs := models.MapRecordToEntity(r)
					if len(entErrs) > 0 {
						for _, err := range entErrs {
							fmt.Printf("	TestFilings, maping error: %v\n", err)
							errs[err.Error()]++
						}
					}
					fmt.Printf("TestFilings: entity: %v\n", entity)
					_, ok := res[entity.ID]
					if ok {
						res[entity.ID] = append(res[entity.ID], entity)
					} else {
						res[entity.ID] = []*models.Entity{entity}
					}
					resCount++
				}
			}
		case err, ok := <-errCh:
			if !ok {
				fmt.Printf("TestFilings: error stream closed, returning resCount: %d, resCount: %d, errCnt: %v\n", resCount, len(res), errs)
				return
			} else {
				if err != nil {
					fmt.Printf("TestFilings, error: %v\n", err)
					errs[err.Error()]++
				}
			}
		}
	}
}
