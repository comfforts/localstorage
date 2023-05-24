package csv

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/comfforts/logger"
)

const TEST_DIR = "data"

func TestAgents(t *testing.T) {
	logger := logger.NewTestAppLogger(TEST_DIR)

	file, err := os.Open("../../data/Agents-sm.csv")
	require.NoError(t, err)

	csvFiler, err := NewCSVFiler(file, logger)
	require.NoError(t, err)
	defer csvFiler.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resCh := make(chan []string)
	errCh := make(chan error)

	errs := map[string]int{}
	var headers []string
	isFirst := true

	go csvFiler.ReadCSVFile(ctx, resCh, errCh)

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("TestAgents: context done, returning. errCnt: %v\n", errs)
			return
		case r, ok := <-resCh:
			if !ok {
				fmt.Printf("TestAgents: resultstream closed, returning. errCnt: %v\n", errs)
				return
			} else {
				if r != nil {
					if isFirst && len(errs) < 1 {
						fmt.Printf("TestAgents: headers: %v, fieldCount: %d\n", r, len(r))
						headers = r
						isFirst = false
					} else {
						if headers == nil {
							fmt.Printf("TestAgents: no headers\n")
							cancel()
						} else {
							fmt.Printf("TestAgents: record: %v, fieldCount: %d\n", r, len(r))
						}
					}
				}
			}
		case err, ok := <-errCh:
			if !ok {
				fmt.Printf("TestAgents: error stream closed, returning errCnt: %v\n", errs)
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
