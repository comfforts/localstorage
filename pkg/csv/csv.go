package csv

import (
	"context"
	"encoding/csv"
	"io"
	"os"
	"sync"

	"github.com/comfforts/errors"
	"github.com/comfforts/logger"
	"go.uber.org/zap"
)

const (
	ERROR_NO_FILE string = "%s doesn't exist"
)

type csvFiler struct {
	*os.File
	reader *csv.Reader
	mu     sync.Mutex
	size   uint64
	logger logger.AppLogger
}

func NewCSVFiler(f *os.File, logger logger.AppLogger) (*csvFiler, error) {
	fs, err := os.Stat(f.Name())
	if err != nil {
		logger.Error("error getting filer file stats", zap.Error(err))
		return nil, errors.WrapError(err, ERROR_NO_FILE, f.Name())
	}
	size := uint64(fs.Size())
	reader := csv.NewReader(f)
	reader.Comma = '|'
	reader.FieldsPerRecord = -1

	return &csvFiler{
		File:   f,
		size:   size,
		reader: reader,
		logger: logger,
	}, nil
}

func (f *csvFiler) ReadCSVFile(ctx context.Context, cancFn context.CancelFunc, resCh chan []string, errCh chan error) {
	f.logger.Info("csv file header", zap.Any("offset", f.reader.InputOffset()))
	header, err := f.reader.Read()
	if err != nil {
		f.logger.Error("error reading csv header", zap.Error(err))
		errCh <- errors.WrapError(err, "error reading csv header")
	}

	f.logger.Info("csv file header", zap.Any("header", header))

	for i := 0; ; i = i + 1 {
		record, err := f.reader.Read()
		if err == io.EOF {
			f.logger.Info("end of csv file")
			errCh <- errors.WrapError(err, "end of csv file")
			close(resCh)
			close(errCh)
			cancFn()
			return
		} else if err != nil {
			f.logger.Error("error reading csv file", zap.Error(err), zap.Any("offset", f.reader.InputOffset()))
			errCh <- errors.WrapError(err, "error reading csv header")
		}

		f.logger.Info("csv row", zap.Int("row", i), zap.Any("record", record), zap.Any("offset", f.reader.InputOffset()))
		select {
		case <-ctx.Done():
			return
		case resCh <- record:
		}
	}
}

func (f *csvFiler) Close() error {
	return f.File.Close()
}
