/*
 * Copyright (C) linvon
 * Date  2021/2/18 10:29
 */

package cuckoo

import "fmt"

const (
	bitsPerByte    = 8
	bytesPerUint64 = 8
	bytesPerUint32 = 4
)

func getNextPow2(n uint64) uint {
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++
	return uint(n)
}

func maxLoadFactor(tagsPerBucket uint) float64 {
	switch tagsPerBucket {
	case 2:
		return 0.85
	case 4:
		return 0.96
	default:
		return 0.99
	}
}

func getBucketsFromHint(initialBucketsHint []byte, expectedLength uint) ([]byte, error) {
	result := initialBucketsHint
	if len(result) == 0 {
		result = make([]byte, expectedLength)
	}
	if uint(len(result)) != expectedLength {
		return nil, fmt.Errorf("buckets length should be %d but got %d", expectedLength, len(result))
	}
	return result, nil
}
