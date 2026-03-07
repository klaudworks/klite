package wal

import "errors"

var (
	ErrCRCMismatch = errors.New("wal: CRC mismatch")
	ErrCorrupted   = errors.New("wal: corrupted entry")
	ErrClosed      = errors.New("wal: writer closed")
)
