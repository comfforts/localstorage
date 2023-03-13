package localstorage

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/comfforts/errors"
	"github.com/comfforts/logger"

	csvFiler "github.com/comfforts/localstorage/pkg/csv"
	jsonFiler "github.com/comfforts/localstorage/pkg/json"
)

const DEFAULT_BUFFER_SIZE = 1000

type JSONMapper = map[string]interface{}

type ReadResponse struct {
	Result JSONMapper
	Error  error
}

type WriteResponse struct {
	Error error
}

type LocalStorage interface {
	ReadJSONFile(ctx context.Context, filePath string, resCh chan JSONMapper, errCh chan error) error
	ReadCSVFile(ctx context.Context, filePath string, resCh chan []string, errCh chan error) error
	ReadFileArray(ctx context.Context, cancel func(), filePath string) (<-chan ReadResponse, error)
	WriteFile(ctx context.Context, cancel func(), fileName string, reqStream chan JSONMapper) <-chan WriteResponse
	Copy(srcPath, destPath string) (int64, error)
	CopyBuf(srcPath, destPath string) (int64, error)
}

type localStorageClient struct {
	logger logger.AppLogger
}

func NewLocalStorageClient(logger logger.AppLogger) (*localStorageClient, error) {
	if logger == nil {
		return nil, errors.NewAppError(errors.ERROR_MISSING_REQUIRED)
	}
	loaderClient := &localStorageClient{
		logger: logger,
	}

	return loaderClient, nil
}

func (lc *localStorageClient) ReadJSONFile(ctx context.Context, filePath string, resCh chan JSONMapper, errCh chan error) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}

	jsonFile, err := jsonFiler.NewJSONFiler(file, lc.logger)
	if err != nil {
		return err
	}

	go jsonFile.ReadJSONFile(ctx, resCh, errCh)
	return nil
}

func (lc *localStorageClient) ReadCSVFile(ctx context.Context, filePath string, resCh chan []string, errCh chan error) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}

	csvFile, err := csvFiler.NewCSVFiler(file, lc.logger)
	if err != nil {
		return err
	}

	go csvFile.ReadCSVFile(ctx, resCh, errCh)
	return nil
}

// ReadFileArray reads an array of json data from existing file, one by one,
// and returns individual result at defined rate through returned channel
func (lc *localStorageClient) ReadFileArray(ctx context.Context, cancel func(), filePath string) (<-chan ReadResponse, error) {
	// checks if file exists
	_, err := fileStats(filePath)
	if err != nil {
		return nil, err
	}

	// Open file
	f, err := os.Open(filePath)
	if err != nil {
		return nil, errors.WrapError(err, ERROR_OPENING_FILE, filePath)
	}

	resultStream := make(chan ReadResponse)
	go lc.readFile(ctx, cancel, filePath, f, resultStream)

	return resultStream, nil
}

func (lc *localStorageClient) WriteFile(ctx context.Context, cancel func(), fileName string, reqStream chan JSONMapper) <-chan WriteResponse {
	filePath := filepath.Join("data", fileName)

	resultStream := make(chan WriteResponse)
	go lc.writeFile(ctx, cancel, filePath, reqStream, resultStream)

	return resultStream
}

func (lc *localStorageClient) Copy(srcPath, destPath string) (int64, error) {
	srcStat, err := os.Stat(srcPath)
	if err != nil {
		return 0, errors.WrapError(err, ERROR_NO_FILE, srcPath)
	}
	if !srcStat.Mode().IsRegular() {
		return 0, errors.WrapError(err, ERROR_NOT_A_FILE, srcPath)
	}

	src, err := os.Open(srcPath)
	if err != nil {
		return 0, errors.WrapError(err, ERROR_OPENING_FILE, srcPath)
	}
	defer src.Close()

	err = createDirectory(destPath)
	if err != nil {
		return 0, err
	}

	dest, err := os.Create(destPath)
	if err != nil {
		return 0, errors.WrapError(err, ERROR_CREATING_FILE, destPath)
	}
	defer dest.Close()

	nBytes, err := io.Copy(dest, src)
	return nBytes, err
}

func (lc *localStorageClient) CopyBuf(srcPath, destPath string) (int64, error) {
	srcStat, err := os.Stat(srcPath)
	if err != nil {
		return 0, errors.WrapError(err, ERROR_NO_FILE, srcPath)
	}
	if !srcStat.Mode().IsRegular() {
		return 0, errors.WrapError(err, ERROR_NOT_A_FILE, srcPath)
	}

	src, err := os.Open(srcPath)
	if err != nil {
		return 0, errors.WrapError(err, ERROR_OPENING_FILE, srcPath)
	}
	defer src.Close()

	err = createDirectory(destPath)
	if err != nil {
		return 0, err
	}

	dest, err := os.Create(destPath)
	if err != nil {
		return 0, errors.WrapError(err, ERROR_CREATING_FILE, destPath)
	}
	defer dest.Close()

	buf := make([]byte, DEFAULT_BUFFER_SIZE)
	var nBytes int64 = 0
	for {
		nr, err := src.Read(buf)
		if err != nil && err != io.EOF {
			return nBytes, errors.WrapError(err, ERROR_READING_FILE, srcPath)
		}
		if nr == 0 {
			break
		}
		nw, err := dest.Write(buf[:nr])
		if err != nil {
			return nBytes, errors.WrapError(err, ERROR_WRITING_FILE, srcPath)
		}
		nBytes = nBytes + int64(nw)
	}

	return nBytes, err
}

func (lc *localStorageClient) readFile(ctx context.Context, cancel func(), filePath string, file io.ReadCloser, rrs chan ReadResponse) {
	defer close(rrs)
	defer func() {
		lc.logger.Info("closing result stream and file")
		if err := file.Close(); err != nil {
			rrs <- ReadResponse{
				Error: errors.WrapError(err, ERROR_CLOSING_FILE, filePath),
			}
		}
	}()

	r := bufio.NewReader(file)
	dec := json.NewDecoder(r)

	// read open bracket
	t, err := dec.Token()
	if err != nil || t != json.Delim('[') {
		rrs <- ReadResponse{
			Error: ErrStartToken,
		}
		cancel()
		return
	}

	// while the array contains values
	for dec.More() {
		var result JSONMapper
		err := dec.Decode(&result)
		var response = ReadResponse{}
		if err != nil {
			response.Error = errors.WrapError(err, ERROR_DECODING_RESULT)
		} else {
			response.Result = result
		}
		select {
		case <-ctx.Done():
			return
		case rrs <- response:
		}
	}

	// read closing bracket
	t, err = dec.Token()
	if err != nil || t != json.Delim(']') {
		rrs <- ReadResponse{
			Error: ErrEndToken,
		}
		cancel()
		return
	}
}

func (lc *localStorageClient) writeFile(ctx context.Context, cancel func(), filePath string, reqStream chan JSONMapper, wrs chan WriteResponse) {
	defer func() {
		lc.logger.Info("closing write response stream")
		close(wrs)
	}()
	file, err := os.OpenFile(filePath, os.O_CREATE, os.ModePerm)
	if err != nil {
		wrs <- WriteResponse{
			Error: errors.WrapError(err, ERROR_CREATING_FILE, filePath),
		}
		cancel()
		return
	}
	defer func() {
		if err := file.Close(); err != nil {
			wrs <- WriteResponse{
				Error: errors.WrapError(err, ERROR_CLOSING_FILE, filePath),
			}
		}
	}()

	jsonData := []JSONMapper{}
	for req := range reqStream {
		jsonData = append(jsonData, req)
	}

	enc := json.NewEncoder(file)
	err = enc.Encode(jsonData)
	if err != nil {
		wrs <- WriteResponse{
			Error: errors.WrapError(err, ERROR_CREATING_FILE, filePath),
		}
		cancel()
		return
	}
}

func createDirectory(path string) error {
	_, err := os.Stat(filepath.Dir(path))
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(filepath.Dir(path), os.ModePerm)
			if err == nil {
				return nil
			}
		}
		return err
	}
	return nil
}

func fileStats(filePath string) (fs.FileInfo, error) {
	fStats, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return fStats, errors.WrapError(err, ERROR_NO_FILE, filePath)
		} else {
			return fStats, errors.WrapError(err, ERROR_FILE_INACCESSIBLE, filePath)
		}
	}
	return fStats, nil
}
