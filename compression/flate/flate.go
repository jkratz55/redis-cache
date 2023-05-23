package flate

import (
	"bytes"
	"compress/flate"
	"io"
)

// Codec is a Compressor that is backed by compress/flate package
// from the standard library. The zero-value is usable but will not perform
// any compression. The compression level can be configured by setting the
// Level field. The valid values are below:
//
//	NoCompression      = 0
//	BestSpeed          = 1
//	BestCompression    = 9
//	DefaultCompression = -1
type Codec struct {
	Level int
}

func (c Codec) Flate(data []byte) ([]byte, error) {
	b := &bytes.Buffer{}
	w, err := flate.NewWriter(b, c.Level)
	if err != nil {
		return nil, err
	}
	if _, err = w.Write(data); err != nil {
		return nil, err
	}
	w.Close()
	return b.Bytes(), nil
}

func (c Codec) Deflate(data []byte) ([]byte, error) {
	buffer := &bytes.Buffer{}
	r := flate.NewReader(bytes.NewReader(data))
	_, err := io.Copy(buffer, r)
	return buffer.Bytes(), err
}
