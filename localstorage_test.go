package localstorage

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

func TestReadJSONFile(t *testing.T) {
	name := "data"
	fPath, err := createJSONFile(TEST_DIR, name)
	require.NoError(t, err)

	logger := logger.NewTestAppLogger(TEST_DIR)
	lsc, err := NewLocalStorageClient(logger)
	require.NoError(t, err)

	resCh := make(chan JSONMapper)
	errCh := make(chan error)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = lsc.ReadJSONFile(ctx, fPath, resCh, errCh)
	require.NoError(t, err)

	errs := map[string]int{}
	resCount := 0
	for {
		select {
		case <-ctx.Done():
			require.Equal(t, resCount, 3)
			require.Equal(t, len(errs), 0)
			return
		case r, ok := <-resCh:
			if !ok {
				require.Equal(t, resCount, 3)
				require.Equal(t, len(errs), 0)
				return
			} else {
				if r != nil {
					t.Logf("TestReadJSONFile: result: %v\n", r)
					resCount++
				}
			}
		case err, ok := <-errCh:
			if !ok {
				require.Equal(t, resCount, 3)
				require.Equal(t, len(errs), 0)
				return
			} else {
				if err != nil {
					t.Logf("TestReadJSONFile - error: %v\n", err)
					errs[err.Error()]++
				}
			}
		}
	}
}

func TestReadCSVFile(t *testing.T) {
	fPath := filepath.Join(TEST_DIR, "Principals.csv")
	// fPath := filepath.Join(TEST_DIR, "Agents.csv")

	logger := logger.NewTestAppLogger(TEST_DIR)
	lsc, err := NewLocalStorageClient(logger)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resCh := make(chan []string)
	errCh := make(chan error)

	err = lsc.ReadCSVFile(ctx, fPath, resCh, errCh)
	require.NoError(t, err)

	errs := map[string]int{}
	res := map[int]*models.Entity{}
	for {
		select {
		case <-ctx.Done():
			t.Logf("TestReadCSVFile: context done, returning. resCount: %d, errCnt: %v\n", len(res), errs)
			return
		case r, ok := <-resCh:
			if !ok {
				t.Logf("TestReadCSVFile: resultstream closed, returning. resCount: %d, errCnt: %v\n", len(res), errs)
				return
			} else {
				if r != nil {
					entity := models.MapToEntity(res, r)
					t.Logf("TestReadCSVFile: entity: %v\n", entity)
				}
			}
		case err, ok := <-errCh:
			if !ok {
				t.Logf("TestReadCSVFile: error stream closed, returning resCount: %d, errCnt: %v\n", len(res), errs)
				return
			} else {
				if err != nil {
					t.Logf("TestReadCSVFile, error: %v\n", err)
					errs[err.Error()]++
				}
			}
		}
	}

}

func TestReadFilingsCSVFile(t *testing.T) {
	fPath := filepath.Join(TEST_DIR, "Filings.csv")

	logger := logger.NewTestAppLogger(TEST_DIR)
	lsc, err := NewLocalStorageClient(logger)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resCh := make(chan []string)
	errCh := make(chan error)

	err = lsc.ReadCSVFile(ctx, fPath, resCh, errCh)
	require.NoError(t, err)

	errs := map[string]int{}
	res := map[int][]*models.Entity{}
	var resCount int
	for {
		select {
		case <-ctx.Done():
			t.Logf("TestReadFilingsCSVFile: context done, returning. recordCount: %d, resCount: %d, errCnt: %v\n", resCount, len(res), errs)
			return
		case r, ok := <-resCh:
			if !ok {
				t.Logf("TestReadFilingsCSVFile: resultstream closed, returning. resCount: %d, resCount: %d, errCnt: %v\n", resCount, len(res), errs)
				return
			} else {
				if r != nil {
					entity, entErrs := models.MapRecordToEntity(r)
					if entErrs != nil && len(entErrs) > 0 {
						for _, err := range entErrs {
							t.Logf("TestReadFilingsCSVFile, maping error: %v\n", err)
							errs[err.Error()]++
						}
					}
					t.Logf("TestReadFilingsCSVFile: entity: %v\n", entity)
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
				t.Logf("TestReadFilingsCSVFile: error stream closed, returning resCount: %d, resCount: %d, errCnt: %v\n", resCount, len(res), errs)
				return
			} else {
				if err != nil {
					t.Logf("TestReadFilingsCSVFile, error: %v\n", err)
					errs[err.Error()]++
				}
			}
		}
	}
}

func TestLocalFileStorage(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client LocalStorage,
		testDir string,
	){
		"local storage file read succeeds":                   testReadFileArray,
		"local storage file read missing token throws error": testReadFileArrayMissingTokens,
		"local storage copy json file succeeds":              testCopy,
		"local storage copy json file buffered succeeds":     testCopyBuffer,
		"file stats test succeeds":                           testFileStats,
		// "read write file array succeeds":                     testReadWriteFileArray,
	} {
		testDir := fmt.Sprintf("%s/", TEST_DIR)
		t.Run(scenario, func(t *testing.T) {
			client, teardown := setupLocalTest(t, testDir)
			defer teardown()
			fn(t, client, testDir)
		})
	}
}

func setupLocalTest(t *testing.T, testDir string) (
	client LocalStorage,
	teardown func(),
) {
	t.Helper()

	err := createDirectory(testDir)
	require.NoError(t, err)

	appLogger := logger.NewTestAppLogger(TEST_DIR)

	lsc, err := NewLocalStorageClient(appLogger)
	require.NoError(t, err)

	return lsc, func() {
		t.Logf(" test ended, will remove %s folder", TEST_DIR)
		err := os.RemoveAll(TEST_DIR)
		require.NoError(t, err)
	}
}

func testReadFileArray(t *testing.T, client LocalStorage, testDir string) {
	ctx, cancel := context.WithCancel(context.Background())
	count := 0
	errCount := 0
	defer func() {
		require.Equal(t, 3, count)
		require.Equal(t, 0, errCount)
		cancel()
	}()

	name := "data"
	fPath, err := createJSONFile(testDir, name)
	require.NoError(t, err)

	resultStream, err := client.ReadFileArray(ctx, cancel, fPath)
	require.NoError(t, err)

	for {
		select {
		case <-ctx.Done():
			return
		case r, ok := <-resultStream:
			if !ok {
				t.Log("	testReadFileArray: resultstream closed, returning")
				return
			} else {
				if r.Result != nil {
					t.Logf(" testReadFileArray: result: %v", r.Result)
					count++
				}
				if r.Error != nil {
					t.Logf(" testReadFileArray: error: %v", r.Error)
					errCount++
				}
			}
		}
	}
}

func testReadFileArrayMissingTokens(t *testing.T, client LocalStorage, testDir string) {
	ctx, cancel := context.WithCancel(context.Background())
	count := 0
	errCount := 0
	defer func() {
		require.Equal(t, 0, count)
		require.Equal(t, 1, errCount)
		cancel()
	}()

	name := "data_missing_tokens"
	fPath, err := createSingleJSONFile(testDir, name)
	require.NoError(t, err)

	resultStream, err := client.ReadFileArray(ctx, cancel, fPath)
	require.NoError(t, err)

	for {
		select {
		case <-ctx.Done():
			return
		case r, ok := <-resultStream:
			if !ok {
				t.Log("	testReadFileArrayMissingTokens: resultstream closed, returning")
				return
			} else {
				if r.Result != nil {
					t.Logf(" testReadFileArrayMissingTokens: result: %v", r.Result)
					count++
				}
				if r.Error != nil {
					t.Logf(" testReadFileArrayMissingTokens: error: %v", r.Error)
					errCount++
				}
			}
		}
	}
}

func testCopyBuffer(t *testing.T, client LocalStorage, testDir string) {
	name := "test"
	srcName, err := createJSONFile(testDir, name)
	require.NoError(t, err)

	destName := fmt.Sprintf("%s/%s-copy-buf.json", testDir, name)
	n, err := client.CopyBuf(srcName, destName)
	require.NoError(t, err)
	t.Logf(" testCopy: %d bytes written", n)
	require.Equal(t, true, n > 0)
}

func testCopy(t *testing.T, client LocalStorage, testDir string) {
	name := "test"
	srcName, err := createJSONFile(testDir, name)
	require.NoError(t, err)

	destName := fmt.Sprintf("%s/%s-copy.json", testDir, name)
	n, err := client.Copy(srcName, destName)
	require.NoError(t, err)
	t.Logf(" testCopy: %d bytes written", n)
	require.Equal(t, true, n > 0)
}

func testFileStats(t *testing.T, client LocalStorage, testDir string) {
	name := "test"
	fPath, err := createJSONFile(testDir, name)
	require.NoError(t, err)

	_, err = fileStats(fPath)
	require.NoError(t, err)
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

func createStoreJSONList() []JSONMapper {
	items := []JSONMapper{
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

func createStoreJSON(storeId uint64, name, org, city, country string) JSONMapper {
	s := JSONMapper{
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

// func testReadWriteFileArray(t *testing.T, client LocalStorage, testDir string) {
// 	rCtx, rCancel := context.WithCancel(context.Background())
// 	defer func() {
// 		rCancel()
// 	}()

// 	name := "data"
// 	fPath, err := createJSONFile(testDir, name)
// 	require.NoError(t, err)

// 	resultStream, err := client.ReadFileArray(rCtx, rCancel, fPath)
// 	require.NoError(t, err)

// 	wCtx, wCancel := context.WithCancel(context.Background())
// 	defer func() {
// 		wCancel()
// 	}()
// 	writeFileName := "dataWrite.json"
// 	requestStream := make(chan Result)
// 	respStream := client.WriteFile(wCtx, wCancel, writeFileName, requestStream)

// 	func() {
// 		defer close(requestStream)
// 		for {
// 			select {
// 			case <-rCtx.Done():
// 				t.Log("rCtx done")
// 			case r, ok := <-resultStream:
// 				if !ok {
// 					t.Log(" testReadWriteFileArray: resultstream closed, returning")
// 					return
// 				} else {
// 					if r.Result != nil {
// 						t.Logf(" testReadWriteFileArray: result: %v", r.Result)
// 						requestStream <- r.Result
// 					}
// 					if r.Error != nil {
// 						t.Logf(" testReadWriteFileArray: error: %v", r.Error)
// 					}
// 				}
// 			}
// 		}
// 	}()

// 	func() {
// 		for {
// 			select {
// 			case <-wCtx.Done():
// 				t.Log("wCtx done, returning")
// 				return
// 			case r, ok := <-respStream:
// 				if !ok {
// 					t.Log("WriteFile resultstream closed, returning")
// 					return
// 				} else {
// 					if r.Error != nil {
// 						t.Logf("WriteFile error: %v", r.Error)
// 					}
// 				}
// 			}
// 		}
// 	}()
// }
