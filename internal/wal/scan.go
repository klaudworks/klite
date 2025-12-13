package wal

import (
	"encoding/binary"
	"io"
)

// ScanFramedEntries reads framed entries from r, calling fn for each valid entry.
// Entry framing: [4 bytes length][payload of that length].
// Stops at EOF, short read, or when fn returns false.
// Returns the number of valid entries scanned and any error.
// On CRC mismatch or truncation, scanning stops (no error returned — the
// caller decides how to handle partial tail data).
func ScanFramedEntries(r io.Reader, fn func(payload []byte) bool) (int, error) {
	var lenBuf [4]byte
	count := 0

	for {
		// Read 4-byte length prefix
		_, err := io.ReadFull(r, lenBuf[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return count, nil // clean end or truncated length
			}
			return count, err
		}

		entryLen := binary.BigEndian.Uint32(lenBuf[:])
		if entryLen == 0 || entryLen > 256*1024*1024 { // sanity: max 256 MiB per entry
			return count, nil // corrupted length, stop scanning
		}

		// Read payload
		payload := make([]byte, entryLen)
		_, err = io.ReadFull(r, payload)
		if err != nil {
			return count, nil // truncated entry, stop scanning
		}

		if !fn(payload) {
			count++
			return count, nil
		}
		count++
	}
}
