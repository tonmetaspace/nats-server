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
	"testing"
	"time"
)

func TestCompressionRoundTrip(t *testing.T) {
	input := []byte("HELLO THERE!")

	for _, ct := range []CompressionType{
		CompressionTypeGZIP,
		CompressionTypeZLIB,
	} {
		c := wrapStreamStoreWithCompression(nil, ct).(*compressedStreamStore)

		compressed, err := c.compress(input)
		if err != nil {
			t.Fatal(err)
		}

		decompressed, err := c.decompress(compressed)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(input, decompressed) {
			t.Fatalf("compression round-trip failed")
		}
	}
}

func TestJetStreamAddCompressedStream(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{
			name: "MemoryStoreNone",
			mconfig: &StreamConfig{
				Name:        "foo",
				Retention:   LimitsPolicy,
				MaxAge:      time.Hour,
				Storage:     MemoryStorage,
				Compression: CompressionTypeNone,
				Replicas:    1,
			},
		},
		{
			name: "MemoryStoreGZIP",
			mconfig: &StreamConfig{
				Name:        "foo",
				Retention:   LimitsPolicy,
				MaxAge:      time.Hour,
				Storage:     MemoryStorage,
				Compression: CompressionTypeGZIP,
				Replicas:    1,
			},
		},
		{
			name: "MemoryStoreZLIB",
			mconfig: &StreamConfig{
				Name:        "foo",
				Retention:   LimitsPolicy,
				MaxAge:      time.Hour,
				Storage:     MemoryStorage,
				Compression: CompressionTypeZLIB,
				Replicas:    1,
			},
		},
		{
			name: "FileStoreNone",
			mconfig: &StreamConfig{
				Name:        "foo",
				Retention:   LimitsPolicy,
				MaxAge:      time.Hour,
				Storage:     FileStorage,
				Compression: CompressionTypeNone,
				Replicas:    1,
			},
		},
		{
			name: "FileStoreGZIP",
			mconfig: &StreamConfig{
				Name:        "foo",
				Retention:   LimitsPolicy,
				MaxAge:      time.Hour,
				Storage:     FileStorage,
				Compression: CompressionTypeGZIP,
				Replicas:    1,
			},
		},
		{
			name: "FileStoreZLIB",
			mconfig: &StreamConfig{
				Name:        "foo",
				Retention:   LimitsPolicy,
				MaxAge:      time.Hour,
				Storage:     FileStorage,
				Compression: CompressionTypeZLIB,
				Replicas:    1,
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Publish something highly compressible
			input := []byte("aaaaaaaaBBBBBBBBccccccccDDDDDDDDeeeeeeeeFFFFFFFF")
			nc.Publish("foo", input)
			nc.Flush()

			state := mset.state()
			if state.Msgs != 1 {
				t.Fatalf("Expected 1 message, got %d", state.Msgs)
			}
			if state.Bytes == 0 {
				t.Fatalf("Expected non-zero bytes")
			}

			switch s := mset.store.(type) {
			case *memStore: // Uncompressed memory store
				if c.mconfig.Compression != CompressionTypeNone {
					t.Fatal("uncompressed memory store found but expected compression")
				}
				msg := s.msgs[state.LastSeq].msg
				if !bytes.Equal(input, msg) {
					t.Fatal("uncompressed message in memory didn't match input")
				}

			case *fileStore: // Uncompressed file store
				if c.mconfig.Compression != CompressionTypeNone {
					t.Fatal("uncompressed file store found but expected compression")
				}
				msg, err := s.msgForSeq(state.LastSeq, nil)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(input, msg.msg) {
					t.Fatal("uncompressed message in file didn't match input")
				}

			case *compressedStreamStore:
				if c.mconfig.Compression == CompressionTypeNone {
					t.Fatal("compressed store found but expected no compression")
				}
				switch s := s.StreamStore.(type) {
				case *memStore: // Compressed memory store
					msg := s.msgs[state.LastSeq].msg
					if bytes.Equal(input, msg) {
						t.Fatal("compressed message in memory shouldn't match input")
					}
					if len(input) < len(msg) {
						t.Fatal("compressed message in memory shouldn't be longer than the input")
					}

				case *fileStore: // Compressed file store
					msg, err := s.msgForSeq(state.LastSeq, nil)
					if err != nil {
						t.Fatal(err)
					}
					if bytes.Equal(input, msg.msg) {
						t.Fatal("compressed message in file shouldn't match input")
					}
					if len(input) < len(msg.msg) {
						t.Fatal("compressed message in file shouldn't be longer than the input")
					}

				default:
					t.Fatal("unknown store type inside compressed stream store")
				}

			default:
				t.Fatal("unknown store type")
			}

			if err := mset.delete(); err != nil {
				t.Fatalf("Got an error deleting the stream: %v", err)
			}
		})
	}
}
