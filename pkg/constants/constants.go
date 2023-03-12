package constants

import "github.com/comfforts/errors"

var (
	ErrConvertingId      = errors.NewAppError("converting id")
	ErrConvertingFileNum = errors.NewAppError("converting file number")
	ErrMissingName       = errors.NewAppError("missing name")
	ErrMissingId         = errors.NewAppError("missing id")

	ErrMissingFName = errors.NewAppError("missing first name")
	ErrMissingMName = errors.NewAppError("missing middle name")
	ErrMissingLName = errors.NewAppError("missing last name")
	ErrMissingAddr  = errors.NewAppError("missing address")
	ErrMissingPos   = errors.NewAppError("missing position")
	ErrMissingType  = errors.NewAppError("missing agent type")
)
