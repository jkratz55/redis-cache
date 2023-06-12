package gzip

import (
	"bytes"
	"compress/gzip"
	"io"
	"sync"
)

type Codec struct {
	level      int
	readerPool sync.Pool
	writerPool sync.Pool
}

func NewCodec(level int) *Codec {
	return &Codec{
		level:      level,
		readerPool: sync.Pool{},
		writerPool: sync.Pool{},
	}
}

func (c *Codec) Flate(data []byte) ([]byte, error) {
	var b bytes.Buffer
	var err error
	writer, _ := c.writerPool.Get().(*gzip.Writer)
	if writer != nil {
		writer.Reset(&b)
	} else {
		writer, err = gzip.NewWriterLevel(&b, c.level)
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
	gzipReader, _ := c.readerPool.Get().(*gzip.Reader)
	if gzipReader != nil {
		err = gzipReader.Reset(reader)
	} else {
		gzipReader, err = gzip.NewReader(reader)
	}

	if err != nil {
		return nil, err
	}

	defer func() {
		c.readerPool.Put(gzipReader)
	}()

	var buffer bytes.Buffer
	_, err = io.Copy(&buffer, gzipReader)
	return buffer.Bytes(), err
}
