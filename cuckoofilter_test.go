/*
 * Copyright (C) linvon
 * Date  2021/2/18 10:29
 */

package cuckoo

import (
	"crypto/rand"
	"io"
	"reflect"
	"testing"
)

const size = 100000

func TestInsertion_Single(t *testing.T) {
	var insertNum uint = 5000
	var hash [32]byte
	cf := NewFilter(4, 8, size, TableTypeSingle)
	a := make([][]byte, insertNum)
	for i := uint(0); i < insertNum; i++ {
		_, _ = io.ReadFull(rand.Reader, hash[:])
		tmp := make([]byte, 32)
		copy(tmp, hash[:])
		a = append(a, tmp)
		cf.Add(hash[:])
	}

	count := cf.numItems
	if count != insertNum {
		t.Errorf("Expected count = %d, instead count = %d", insertNum, count)
	}

	for _, v := range a {
		cf.Delete(v)
	}

	count = cf.numItems
	if count != 0 {
		t.Errorf("Expected count = 0, instead count == %d", count)
	}
}

func TestInsertion_Packed(t *testing.T) {
	var insertNum uint = 5000
	var hash [32]byte
	cf := NewFilter(4, 9, size, TableTypePacked)
	a := make([][]byte, insertNum)
	for i := uint(0); i < insertNum; i++ {
		_, _ = io.ReadFull(rand.Reader, hash[:])
		tmp := make([]byte, 32)
		copy(tmp, hash[:])
		a = append(a, tmp)
		cf.Add(hash[:])
	}

	count := cf.numItems
	if count != insertNum {
		t.Errorf("Expected count = %d, instead count = %d", insertNum, count)
	}

	for _, v := range a {
		cf.Delete(v)
	}

	count = cf.numItems
	if count != 0 {
		t.Errorf("Expected count = 0, instead count == %d", count)
	}
}

func TestEncodeDecode_Single(t *testing.T) {
	cf := NewFilter(4, 8, size, TableTypeSingle)
	var hash [32]byte
	for i := 0; i < 5; i++ {
		_, _ = io.ReadFull(rand.Reader, hash[:])
		cf.Add(hash[:])
	}
	bytes := cf.Encode()
	ncf, err := Decode(bytes)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !reflect.DeepEqual(cf, ncf) {
		t.Errorf("Expected %v, got %v", cf, ncf)
	}
}

func TestEncodeDecode_Packed(t *testing.T) {
	cf := NewFilter(4, 9, size, TableTypePacked)
	var hash [32]byte
	for i := 0; i < 5; i++ {
		_, _ = io.ReadFull(rand.Reader, hash[:])
		cf.Add(hash[:])
	}
	bytes := cf.Encode()
	ncf, err := Decode(bytes)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !reflect.DeepEqual(cf, ncf) {
		t.Errorf("Expected %v, got %v", cf, ncf)
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
