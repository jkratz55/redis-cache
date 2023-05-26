package lz4

import (
	"bytes"
	"io"
	"sync"

	"github.com/pierrec/lz4/v4"
)

type Codec struct {
	readerPool sync.Pool
	writerPool sync.Pool
}

func NewCodec() *Codec {
	return &Codec{
		readerPool: sync.Pool{},
		writerPool: sync.Pool{},
	}
}

func (c *Codec) Flate(data []byte) ([]byte, error) {
	var buffer bytes.Buffer
	lz4Writer, _ := c.writerPool.Get().(*lz4.Writer)
	if lz4Writer != nil {
		lz4Writer.Reset(&buffer)
	} else {
		lz4Writer = lz4.NewWriter(&buffer)
	}

	defer func() {
		c.writerPool.Put(lz4Writer)
	}()

	_, err := lz4Writer.Write(data)
	if err != nil {
		return nil, err
	}
	err = lz4Writer.Close()
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), err
}

func (c *Codec) Deflate(data []byte) ([]byte, error) {
	reader := bytes.NewReader(data)
	lz4Reader, _ := c.readerPool.Get().(*lz4.Reader)
	if lz4Reader != nil {
		lz4Reader.Reset(reader)
	} else {
		lz4Reader = lz4.NewReader(reader)
	}

	defer func() {
		c.readerPool.Put(lz4Reader)
	}()

	var buffer bytes.Buffer
	_, err := io.Copy(&buffer, lz4Reader)
	return buffer.Bytes(), err
}
