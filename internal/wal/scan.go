package wal

import (
	"encoding/binary"
	"io"
)

// ScanFramedEntries reads [4-byte length][payload] frames from r.
// On CRC mismatch or truncation, scanning stops with no error — the
// caller decides how to handle partial tail data.
func ScanFramedEntries(r io.Reader, fn func(payload []byte) bool) (int, error) {
	var lenBuf [4]byte
	count := 0

	for {
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
