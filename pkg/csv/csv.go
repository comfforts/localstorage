package csv

import (
	"context"
	"encoding/csv"
	"io"
	"os"

	"github.com/comfforts/errors"
	"github.com/comfforts/logger"
	"go.uber.org/zap"
)

const (
	ERR_FILE        string = "%s doesn't exist"
	ERR_NO_FILE     string = "file doesn't exist"
	ERR_CSV_HEADERS string = "error reading csv headers"
	ERR_CSV_RECORD  string = "error reading csv record"
)

type csvFiler struct {
	*os.File
	reader *csv.Reader
	size   uint64
	logger logger.AppLogger
}

func NewCSVFiler(f *os.File, logger logger.AppLogger) (*csvFiler, error) {
	fs, err := os.Stat(f.Name())
	if err != nil {
		logger.Error(ERR_NO_FILE, zap.Error(err))
		return nil, errors.WrapError(err, ERR_FILE, f.Name())
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

// ReadCSVFile takes context, []string res chan & err chan
// sends headers as first result to res chan and records afterwards
// sends errors on err channel
// closes res and err channels on done
func (f *csvFiler) ReadCSVFile(ctx context.Context, resCh chan []string, errCh chan error) {
	defer func() {
		close(resCh)
		close(errCh)
	}()

	f.logger.Info("csv file: reading headers", zap.Any("offset", f.reader.InputOffset()))
	headers, err := f.reader.Read()
	if err != nil {
		f.logger.Error(ERR_CSV_HEADERS, zap.Error(err))
		errCh <- errors.WrapError(err, ERR_CSV_HEADERS)
	}
	resCh <- headers

	f.logger.Info("csv file: start reading records", zap.Any("offset", f.reader.InputOffset()))
	for i := 0; ; i = i + 1 {
		record, err := f.reader.Read()
		if err == io.EOF {
			f.logger.Info("csv file: end of csv file")
			return
		} else if err != nil {
			f.logger.Error(ERR_CSV_RECORD, zap.Error(err), zap.Any("offset", f.reader.InputOffset()))
			errCh <- errors.WrapError(err, ERR_CSV_RECORD)
		}

		select {
		case <-ctx.Done():
			return
		case resCh <- record:
		}
	}
}

func (f *csvFiler) Close() error {
	f.logger.Info("closing filer", zap.Any("offset", f.reader.InputOffset()))
	return f.File.Close()
}
