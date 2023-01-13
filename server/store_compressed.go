// Copyright 2019-2023 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"fmt"
	"io"
)

type CompressionType int

const (
	CompressionTypeNone CompressionType = iota
	CompressionTypeGZIP
	CompressionTypeZLIB
)

type compressedStreamStore struct {
	StreamStore
	reader func(io.Reader) (io.ReadCloser, error)
	writer func(io.Writer) (io.WriteCloser, error)
}

func wrapStreamStoreWithCompression(s StreamStore, ct CompressionType) StreamStore {
	switch ct {
	case CompressionTypeNone:
		return s

	case CompressionTypeGZIP:
		return &compressedStreamStore{
			StreamStore: s,
			reader: func(r io.Reader) (io.ReadCloser, error) {
				return gzip.NewReader(r)
			},
			writer: func(w io.Writer) (io.WriteCloser, error) {
				return gzip.NewWriter(w), nil
			},
		}

	case CompressionTypeZLIB:
		return &compressedStreamStore{
			StreamStore: s,
			reader: func(r io.Reader) (io.ReadCloser, error) {
				return zlib.NewReader(r)
			},
			writer: func(w io.Writer) (io.WriteCloser, error) {
				return zlib.NewWriter(w), nil
			},
		}

	default:
		return s
	}
}

func (c *compressedStreamStore) compress(b []byte) ([]byte, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var buf bytes.Buffer
	writer, err := c.writer(&buf)
	if err != nil {
		return nil, err
	}
	if n, err := writer.Write(b); err != nil {
		return nil, err
	} else if n != len(b) {
		return nil, fmt.Errorf("short write")
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *compressedStreamStore) decompress(b []byte) ([]byte, error) {
	if len(b) == 0 {
		return nil, nil
	}
	in := bytes.NewReader(b)
	reader, err := c.reader(in)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func (c *compressedStreamStore) StoreMsg(subject string, hdr, msg []byte) (uint64, int64, error) {
	var err error
	if hdr, err = c.compress(hdr); err != nil {
		return 0, 0, err
	}
	if msg, err = c.compress(msg); err != nil {
		return 0, 0, err
	}
	return c.StreamStore.StoreMsg(subject, hdr, msg)
}

func (c *compressedStreamStore) StoreRawMsg(subject string, hdr, msg []byte, seq uint64, ts int64) error {
	var err error
	if hdr, err = c.compress(hdr); err != nil {
		return err
	}
	if msg, err = c.compress(msg); err != nil {
		return err
	}
	return c.StreamStore.StoreRawMsg(subject, hdr, msg, seq, ts)
}

func (c *compressedStreamStore) LoadMsg(seq uint64, sm *StoreMsg) (smr *StoreMsg, err error) {
	if smr, err = c.StreamStore.LoadMsg(seq, sm); err != nil {
		return
	}
	if smr.hdr, err = c.decompress(smr.hdr); err != nil {
		return
	}
	if smr.msg, err = c.decompress(smr.msg); err != nil {
		return
	}
	*sm = *smr // side-effect sm?
	return
}

func (c *compressedStreamStore) LoadNextMsg(filter string, wc bool, start uint64, smp *StoreMsg) (sm *StoreMsg, skip uint64, err error) {
	if sm, skip, err = c.StreamStore.LoadNextMsg(filter, wc, start, smp); err != nil {
		return
	}
	if sm.hdr, err = c.decompress(sm.hdr); err != nil {
		return
	}
	if sm.msg, err = c.decompress(sm.msg); err != nil {
		return
	}
	*smp = *sm // side-effect smp?
	return
}

func (c *compressedStreamStore) LoadLastMsg(subject string, sm *StoreMsg) (smr *StoreMsg, err error) {
	if smr, err = c.StreamStore.LoadLastMsg(subject, sm); err != nil {
		return
	}
	if smr.hdr, err = c.decompress(smr.hdr); err != nil {
		return
	}
	if smr.msg, err = c.decompress(smr.msg); err != nil {
		return
	}
	*sm = *smr // side-effect sm?
	return
}