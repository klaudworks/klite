package wal

import "errors"

var (
	// ErrCRCMismatch indicates a corrupted WAL entry.
	ErrCRCMismatch = errors.New("wal: CRC mismatch")

	// ErrCorrupted indicates the WAL segment is corrupted at the scanned position.
	ErrCorrupted = errors.New("wal: corrupted entry")

	// ErrClosed indicates the WAL writer has been stopped.
	ErrClosed = errors.New("wal: writer closed")
)
