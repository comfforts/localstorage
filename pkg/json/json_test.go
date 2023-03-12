package json

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/comfforts/logger"
	"github.com/stretchr/testify/require"

	"github.com/comfforts/localstorage/pkg/models"
)

const TEST_DIR = "data"

func TestReadJSONArray(t *testing.T) {
	logger := logger.NewTestAppLogger(TEST_DIR)

	name := "data"
	fPath, err := createJSONFile(TEST_DIR, name)
	require.NoError(t, err)

	file, err := os.Open(fPath)
	require.NoError(t, err)

	jsonFiler, err := NewJSONFiler(file, logger)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resCh := make(chan models.JSONMapper)
	errCh := make(chan error)

	go jsonFiler.ReadJSONFile(ctx, cancel, resCh, errCh)

	errs := map[string]int{}
	resCount := 0
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("TestReadJSONArray: context done, returning. resCount: %d, errCnt: %v\n", resCount, errs)
			return
		case r, ok := <-resCh:
			if !ok {
				fmt.Printf("TestReadJSONArray: resultstream closed, returning. resCount: %d, errCnt: %v\n", resCount, errs)
				return
			} else {
				if r != nil {
					fmt.Printf("TestReadJSONArray: result: %v\n", r)
					resCount++
				}
			}
		case err, ok := <-errCh:
			if !ok {
				fmt.Printf("TestReadJSONArray: error stream closed, returning resCount: %d, errCnt: %v\n", resCount, errs)
				return
			} else {
				if err != nil {
					fmt.Printf("TestReadJSONArray - error: %v\n", err)
					errs[err.Error()]++
				}
			}
		}
	}
}

func createJSONFile(dir, name string) (string, error) {
	fPath := fmt.Sprintf("%s.json", name)
	if dir != "" {
		fPath = fmt.Sprintf("%s/%s", dir, fPath)
	}

	err := os.MkdirAll(filepath.Dir(fPath), os.ModePerm)
	if err != nil {
		return "", err
	}

	items := createStoreJSONList()

	f, err := os.Create(fPath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	err = encoder.Encode(items)
	if err != nil {
		return "", err
	}
	return fPath, nil
}

func createStoreJSONList() []models.JSONMapper {
	items := []models.JSONMapper{
		{
			"city":      "Hong Kong",
			"org":       "starbucks",
			"name":      "Plaza Hollywood",
			"country":   "CN",
			"longitude": 114.20169067382812,
			"latitude":  22.340700149536133,
			"store_id":  1,
		},
		{
			"city":      "Hong Kong",
			"org":       "starbucks",
			"name":      "Exchange Square",
			"country":   "CN",
			"longitude": 114.15818786621094,
			"latitude":  22.283939361572266,
			"store_id":  6,
		},
		{
			"city":      "Kowloon",
			"org":       "starbucks",
			"name":      "Telford Plaza",
			"country":   "CN",
			"longitude": 114.21343994140625,
			"latitude":  22.3228702545166,
			"store_id":  8,
		},
	}
	return items
}

func createSingleJSONFile(dir, name string) (string, error) {
	fPath := fmt.Sprintf("%s.json", name)
	if dir != "" {
		fPath = fmt.Sprintf("%s/%s", dir, fPath)
	}

	item := createStoreJSON(uint64(1), "Mustum Bugdum", "starbucks", "Hong Kong", "CN")

	f, err := os.Create(fPath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	err = encoder.Encode(item)
	if err != nil {
		return "", err
	}
	return fPath, nil
}

func createStoreJSON(storeId uint64, name, org, city, country string) models.JSONMapper {
	s := models.JSONMapper{
		"name":      name,
		"org":       org,
		"city":      city,
		"country":   country,
		"longitude": 114.74169067382812,
		"latitude":  21.340700149536133,
		"store_id":  storeId,
	}
	return s
}
