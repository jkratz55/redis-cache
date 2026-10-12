package flate

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"sync"

	cache "github.com/jkratz55/redis-cache/v2"
)

// Codec is a Compressor that is backed by compress/flate package
// from the standard library. The zero-value is usable but will not perform
// any compression. The compression level can be configured by setting the
// Level field or by using NewCodec, which validates the level. The valid values
// are below:
//
//	HuffmanOnly        = -2
//	DefaultCompression = -1
//	NoCompression      = 0
//	BestSpeed          = 1
//	BestCompression    = 9
//
// Writers and readers are pooled and reused. Level must not be changed after the
// Codec is first used. Codec is safe for concurrent use.
type Codec struct {
	Level int

	writerPool sync.Pool
	readerPool sync.Pool
}

// NewCodec creates a new Codec with the specified compression level. It panics
// if the level is not in the range flate.HuffmanOnly to flate.BestCompression.
func NewCodec(level int) *Codec {
	if level < flate.HuffmanOnly || level > flate.BestCompression {
		panic(fmt.Errorf("flate: invalid compression level %d", level))
	}
	return &Codec{Level: level}
}

func (c *Codec) Compress(data []byte) ([]byte, error) {
	var b bytes.Buffer
	w, _ := c.writerPool.Get().(*flate.Writer)
	if w != nil {
		w.Reset(&b)
	} else {
		var err error
		w, err = flate.NewWriter(&b, c.Level)
		if err != nil {
			return nil, err
		}
	}

	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	c.writerPool.Put(w)
	return b.Bytes(), nil
}

func (c *Codec) Decompress(data []byte) ([]byte, error) {
	src := bytes.NewReader(data)
	r, _ := c.readerPool.Get().(io.ReadCloser)
	if r != nil {
		if err := r.(flate.Resetter).Reset(src, nil); err != nil {
			return nil, err
		}
	} else {
		r = flate.NewReader(src)
	}

	var buffer bytes.Buffer
	if _, err := io.Copy(&buffer, r); err != nil {
		return nil, err
	}
	if err := r.Close(); err != nil {
		return nil, err
	}
	c.readerPool.Put(r)
	return buffer.Bytes(), nil
}

// FormatID returns cache.CodecFlate.
func (c *Codec) FormatID() uint8 {
	return cache.CodecFlate
}
