package localstorage

import "github.com/comfforts/errors"

const (
	ERROR_NO_FILE           string = "%s doesn't exist"
	ERROR_FILE_INACCESSIBLE string = "%s inaccessible"
	ERROR_NOT_A_FILE        string = "%s not a file"
	ERROR_OPENING_FILE      string = "opening file %s"
	ERROR_READING_FILE      string = "reading file %s"
	ERROR_DECODING_RESULT   string = "error decoding result json"
	ERROR_START_TOKEN       string = "error reading start token"
	ERROR_END_TOKEN         string = "error reading end token"
	ERROR_CLOSING_FILE      string = "closing file %s"
	ERROR_CREATING_FILE     string = "creating file %s"
	ERROR_WRITING_FILE      string = "writing file %s"
)

var (
	ErrStartToken = errors.NewAppError(ERROR_START_TOKEN)
	ErrEndToken   = errors.NewAppError(ERROR_END_TOKEN)
)
