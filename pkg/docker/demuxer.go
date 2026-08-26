package docker

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	StreamTypeStdin  = 0
	StreamTypeStdout = 1
	StreamTypeStderr = 2
)

// LogEntry holds a demultiplexed line of container log output.
type LogEntry struct {
	Container string `json:"container"`
	Stream    string `json:"stream"` // "stdout" or "stderr"
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
}

// DemuxStream parses Docker's 8-byte multiplexed frame format:
// [1 byte stream type] [3 bytes reserved] [4 bytes payload length big-endian]
func DemuxStream(r io.Reader, onLog func(streamType int, payload []byte)) error {
	header := make([]byte, 8)

	for {
		// Read 8-byte frame header
		_, err := io.ReadFull(r, header)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return fmt.Errorf("failed reading docker frame header: %w", err)
		}

		streamType := int(header[0])
		payloadLen := binary.BigEndian.Uint32(header[4:8])

		if payloadLen == 0 {
			continue
		}

		// Read payload bytes
		payload := make([]byte, payloadLen)
		_, err = io.ReadFull(r, payload)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return fmt.Errorf("failed reading docker frame payload: %w", err)
		}

		if onLog != nil {
			onLog(streamType, payload)
		}
	}
}

// EncodeDockerFrame writes an 8-byte header frame followed by payload (useful for testing & mocks).
func EncodeDockerFrame(streamType byte, payload []byte) []byte {
	frame := make([]byte, 8+len(payload))
	frame[0] = streamType
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(payload)))
	copy(frame[8:], payload)
	return frame
}
