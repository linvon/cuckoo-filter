/*
 * Copyright (C) linvon
 * Date  2021/2/18 10:29
 */

package cuckoo

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"reflect"
	"testing"
)

const size = 100000

var (
	testBucketSize      = []uint{2, 4, 8}
	testFingerprintSize = []uint{2, 4, 5, 6, 7, 8, 9, 10, 12, 13, 16, 17, 23, 31, 32}
	testTableType       = []uint{TableTypeSingle, TableTypePacked}
)

func TestFilter(t *testing.T) {
	var insertNum uint = 50000
	var hash [32]byte

	for _, b := range testBucketSize {
		for _, f := range testFingerprintSize {
			for _, table := range testTableType {
				if f == 2 && table == TableTypePacked {
					continue
				}
				if table == TableTypePacked && b != 4 {
					continue
				}
				cf := NewFilter(b, f, 8190, table)
				// fmt.Println(cf.Info())
				a := make([][]byte, 0)
				for i := uint(0); i < insertNum; i++ {
					_, _ = io.ReadFull(rand.Reader, hash[:])
					if cf.AddUnique(hash[:]) {
						tmp := make([]byte, 32)
						copy(tmp, hash[:])
						a = append(a, tmp)
					}
				}

				count := cf.Size()
				if count != uint(len(a)) {
					t.Errorf("Expected count = %d, instead count = %d, b %v f %v", uint(len(a)), count, b, f)
					return
				}

				encodedBytes, err := cf.Encode()
				if err != nil {
					t.Fatalf("err %v", err)
				}
				if len(encodedBytes) != cap(encodedBytes) {
					t.Fatalf("len(%d) != cap(%d)", len(encodedBytes), cap(encodedBytes))
				}
				ncf, err := Decode(encodedBytes)
				if err != nil || !reflect.DeepEqual(cf, ncf) {
					t.Errorf("Expected epual, err %v", err)
					return
				}

				encodedBytes, err = cf.Encode()
				if err != nil {
					t.Fatalf("err %v", err)
				}
				ncf, err = DecodeFrom(encodedBytes)
				if err != nil || !reflect.DeepEqual(cf, ncf) {
					t.Errorf("Expected epual, err %v", err)
					return
				}

				filterReader, _ := cf.EncodeReader()
				bytesFromReader, err := io.ReadAll(filterReader)
				if err != nil {
					t.Fatalf("Error reading from reader")
				}
				if !bytes.Equal(bytesFromReader, encodedBytes) {
					t.Fatalf("Expected to be equal")
				}

				fmt.Println(cf.Info())
				cf.BitsPerItem()
				cf.SizeInBytes()
				cf.LoadFactor()

				for _, v := range a {
					if !cf.Contain(v) {
						t.Errorf("Expected contain, instead not contain, b %v f %v table type %v", b, f, table)
						return
					}
					cf.Delete(v)
				}

				count = cf.Size()
				if count != 0 {
					t.Errorf("Expected count = 0, instead count == %d, b %v f %v table type %v", count, b, f, table)
					return
				}

				fmt.Printf("Filter bucketSize %v fingerprintSize %v tableType %v falsePositive Rate %v \n", b, f, table, cf.FalsePositiveRate())
			}
		}
	}
}

func BenchmarkFilterSingle_Reset(b *testing.B) {
	filter := NewFilter(4, 8, size, TableTypeSingle)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		filter.Reset()
	}
}

func BenchmarkFilterSingle_Insert(b *testing.B) {
	filter := NewFilter(4, 8, size, TableTypeSingle)

	b.ResetTimer()

	var hash [32]byte
	for i := 0; i < b.N; i++ {
		_, _ = io.ReadFull(rand.Reader, hash[:])
		filter.Add(hash[:])
	}
}

func BenchmarkFilterSingle_Lookup(b *testing.B) {
	filter := NewFilter(4, 8, size, TableTypeSingle)

	var hash [32]byte
	for i := 0; i < size; i++ {
		_, _ = io.ReadFull(rand.Reader, hash[:])
		filter.Add(hash[:])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = io.ReadFull(rand.Reader, hash[:])
		filter.Contain(hash[:])
	}
}

func BenchmarkFilterPacked_Reset(b *testing.B) {
	filter := NewFilter(4, 9, size, TableTypePacked)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		filter.Reset()
	}
}

func BenchmarkFilterPacked_Insert(b *testing.B) {
	filter := NewFilter(4, 9, size, TableTypePacked)

	b.ResetTimer()

	var hash [32]byte
	for i := 0; i < b.N; i++ {
		_, _ = io.ReadFull(rand.Reader, hash[:])
		filter.Add(hash[:])
	}
}

func BenchmarkFilterPacked_Lookup(b *testing.B) {
	filter := NewFilter(4, 9, size, TableTypePacked)

	var hash [32]byte
	for i := 0; i < size; i++ {
		_, _ = io.ReadFull(rand.Reader, hash[:])
		filter.Add(hash[:])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = io.ReadFull(rand.Reader, hash[:])
		filter.Contain(hash[:])
	}
}
