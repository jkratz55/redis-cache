package brotli

import (
	"bytes"
	"io"
	"sync"

	"github.com/andybalholm/brotli"
)

type Codec struct {
	level      int
	readerPool sync.Pool
	writerPool sync.Pool
}

func NewCodec(lvl int) *Codec {
	return &Codec{
		level:      lvl,
		readerPool: sync.Pool{},
		writerPool: sync.Pool{},
	}
}

func (c *Codec) Flate(data []byte) ([]byte, error) {
	var b bytes.Buffer
	var err error
	writer, _ := c.writerPool.Get().(*brotli.Writer)
	if writer != nil {
		writer.Reset(&b)
	} else {
		writer = brotli.NewWriterLevel(&b, c.level)
	}

	if err != nil {
		return nil, err
	}

	defer func() {
		c.writerPool.Put(writer)
	}()

	if _, err = writer.Write(data); err != nil {
		return nil, err
	}
	if err = writer.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (c *Codec) Deflate(data []byte) ([]byte, error) {
	var err error
	reader := bytes.NewReader(data)
	brotliReader, _ := c.readerPool.Get().(*brotli.Reader)
	if brotliReader != nil {
		err = brotliReader.Reset(reader)
	} else {
		brotliReader = brotli.NewReader(reader)
	}

	if err != nil {
		return nil, err
	}

	defer func() {
		c.readerPool.Put(reader)
	}()

	var buffer bytes.Buffer
	_, err = io.Copy(&buffer, brotliReader)
	return buffer.Bytes(), err
}
