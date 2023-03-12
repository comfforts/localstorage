package json

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"sync"

	"github.com/comfforts/errors"
	"github.com/comfforts/logger"
	"go.uber.org/zap"

	"github.com/comfforts/localstorage/pkg/models"
)

const (
	ERROR_NO_FILE         string = "%s doesn't exist"
	ERROR_START_TOKEN     string = "error reading start token"
	ERROR_END_TOKEN       string = "error reading end token"
	ERROR_DECODING_RESULT string = "error decoding result json"
)

var (
	ErrStartToken = errors.NewAppError(ERROR_START_TOKEN)
	ErrEndToken   = errors.NewAppError(ERROR_END_TOKEN)
)

type jsonFiler struct {
	*os.File
	reader *bufio.Reader
	mu     sync.Mutex
	size   uint64
	logger logger.AppLogger
}

func NewJSONFiler(f *os.File, logger logger.AppLogger) (*jsonFiler, error) {
	fs, err := os.Stat(f.Name())
	if err != nil {
		logger.Error("error getting filer file stats", zap.Error(err))
		return nil, errors.WrapError(err, ERROR_NO_FILE, f.Name())
	}
	size := uint64(fs.Size())
	reader := bufio.NewReader(f)

	return &jsonFiler{
		File:   f,
		size:   size,
		reader: reader,
		logger: logger,
	}, nil
}

func (f *jsonFiler) ReadJSONFile(ctx context.Context, cancFn context.CancelFunc, resCh chan models.JSONMapper, errCh chan error) {
	dec := json.NewDecoder(f.reader)

	// read open bracket
	t, err := dec.Token()
	if err != nil || t != json.Delim('[') {
		errCh <- ErrStartToken
		close(resCh)
		close(errCh)
		cancFn()
		return
	}

	// while the array contains values
	for dec.More() {
		var result models.JSONMapper
		err := dec.Decode(&result)
		if err != nil {
			errCh <- errors.WrapError(err, ERROR_DECODING_RESULT)
		}
		select {
		case <-ctx.Done():
			return
		case resCh <- result:
		}
	}

	// read closing bracket
	t, err = dec.Token()
	if err != nil || t != json.Delim(']') {
		errCh <- ErrEndToken
	}

	close(resCh)
	close(errCh)
	cancFn()
}

func (f *jsonFiler) Close() error {
	return f.File.Close()
}
