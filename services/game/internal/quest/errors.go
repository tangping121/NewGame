package quest

import "errors"

var (
	ErrAlreadyAccepted = errors.New("quest already accepted")
	ErrNotAccepted     = errors.New("quest not accepted")
)
