/*
 * Copyright (C) linvon
 * Date  2021/2/18 10:29
 */

package cuckoo

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
)

// SingleTable the most naive table implementation: one huge bit array
type SingleTable struct {
	kTagsPerBucket uint
	numBuckets     uint64
	bitsPerTag     uint
	tagMask        uint32
	bucket         []byte
	len            uint64
	totalSlots     uint64
}

// NewSingleTable return a singleTable
func NewSingleTable() *SingleTable {
	return &SingleTable{}
}

// Init init table
func (t *SingleTable) Init(tagsPerBucket, bitsPerTag uint, num uint64, initialBucketsHint []byte) error {
	t.bitsPerTag = bitsPerTag
	t.numBuckets = num
	t.kTagsPerBucket = tagsPerBucket

	t.tagMask = (1 << bitsPerTag) - 1
	t.len = (uint64(t.bitsPerTag)*uint64(t.kTagsPerBucket)*t.numBuckets + 7) >> 3
	buckets, err := getBucketsFromHint(initialBucketsHint, t.len)
	if err != nil {
		return err
	}
	t.bucket = buckets
	t.totalSlots = uint64(t.kTagsPerBucket) * t.numBuckets
	return nil
}

// NumBuckets return num of table buckets
func (t *SingleTable) NumBuckets() uint64 {
	return t.numBuckets
}

// SizeInBytes return bytes occupancy of table
func (t *SingleTable) SizeInBytes() uint64 {
	return t.len
}

// SizeInTags return num of tags that table can store
func (t *SingleTable) SizeInTags() uint64 {
	return t.totalSlots
}

// BitsPerItem return bits occupancy per item of table
func (t *SingleTable) BitsPerItem() uint {
	return t.bitsPerTag
}

// ReadTag read tag from bucket(i,j)
func (t *SingleTable) ReadTag(i, j uint64) uint32 {
	pos := (i*uint64(t.bitsPerTag)*uint64(t.kTagsPerBucket) + uint64(t.bitsPerTag)*j) / bitsPerByte
	var tag uint32
	/* following code only works for little-endian */
	switch t.bitsPerTag {
	case 2:
		shift := j & (4 - 1)
		tag = uint32(t.bucket[pos]) >> (2 * shift)
	case 4:
		tag = uint32(t.bucket[pos]) >> ((j & 1) << 2)
	case 8:
		tag = uint32(t.bucket[pos])
	case 12:
		tag = (uint32(t.bucket[pos]) | uint32(t.bucket[pos+1])<<8) >> ((j & 1) << 2)
	case 16:
		tag = uint32(t.bucket[pos]) | uint32(t.bucket[pos+1])<<8
	case 32:
		tag = uint32(t.bucket[pos]) | uint32(t.bucket[pos+1])<<8 | uint32(t.bucket[pos+2])<<16 | uint32(t.bucket[pos+3])<<24
	default:
		tag = t.readOutBytes(i, j, pos)
	}
	return tag & t.tagMask
}

func (t *SingleTable) readOutBytes(i, j, pos uint64) uint32 {
	rShift := (i*uint64(t.bitsPerTag)*uint64(t.kTagsPerBucket) + uint64(t.bitsPerTag)*j) & (bitsPerByte - 1)
	// tag is max 32bit, so max occupies 5 bytes
	kBytes := (rShift + uint64(t.bitsPerTag) + 7) / bitsPerByte
	var tmp uint64
	for k := uint64(0); k < kBytes; k++ {
		tmp |= uint64(t.bucket[pos+k]) << (bitsPerByte * k)
	}
	tmp >>= rShift
	return uint32(tmp)
}

// WriteTag write tag into bucket(i,j)
func (t *SingleTable) WriteTag(i, j uint64, n uint32) {
	pos := (i*uint64(t.bitsPerTag)*uint64(t.kTagsPerBucket) + uint64(t.bitsPerTag)*j) / bitsPerByte
	tag := n & t.tagMask
	/* following code only works for little-endian */
	switch t.bitsPerTag {
	case 2:
		shift := j & (4 - 1)
		t.bucket[pos] &= ^(0x03 << (2 * shift))
		t.bucket[pos] |= uint8(tag) << (2 * shift)
	case 4:
		if (j & 1) == 0 {
			t.bucket[pos] &= 0xf0
			t.bucket[pos] |= uint8(tag)
		} else {
			t.bucket[pos] &= 0x0f
			t.bucket[pos] |= uint8(tag) << 4
		}
	case 8:
		t.bucket[pos] = uint8(tag)
	case 12:
		var tmp uint16
		tmp = uint16(t.bucket[pos]) | uint16(t.bucket[pos+1])<<8
		if (j & 1) == 0 {
			tmp &= 0xf000
			tmp |= uint16(tag)
		} else {
			tmp &= 0x000f
			tmp |= uint16(tag) << 4
		}
		t.bucket[pos] = byte(tmp)
		t.bucket[pos+1] = byte(tmp >> 8)
	case 16:
		t.bucket[pos] = byte(tag)
		t.bucket[pos+1] = byte(tag >> 8)
	case 32:
		t.bucket[pos] = byte(tag)
		t.bucket[pos+1] = byte(tag >> 8)
		t.bucket[pos+2] = byte(tag >> 16)
		t.bucket[pos+3] = byte(tag >> 24)
	default:
		t.writeInBytes(i, j, pos, tag)
	}
}

func (t *SingleTable) writeInBytes(i, j, pos uint64, tag uint32) {
	rShift := (i*uint64(t.bitsPerTag)*uint64(t.kTagsPerBucket) + uint64(t.bitsPerTag)*j) & (bitsPerByte - 1)
	lShift := (rShift + uint64(t.bitsPerTag)) & (bitsPerByte - 1)
	// tag is max 32bit, so max occupies 5 bytes
	kBytes := (rShift + uint64(t.bitsPerTag) + 7) / bitsPerByte

	rMask := uint8(0xff) >> (bitsPerByte - rShift)
	lMask := uint8(0xff) << lShift
	if lShift == 0 {
		lMask = uint8(0)
	}
	var tmp uint64
	tmp |= uint64(t.bucket[pos] & rMask)
	end := kBytes - 1
	tmp |= uint64(t.bucket[pos+end]&lMask) << (end * bitsPerByte)
	tmp |= uint64(tag) << rShift

	for k := uint64(0); k < kBytes; k++ {
		t.bucket[pos+k] = byte(tmp >> (k * bitsPerByte))
	}
}

// FindTagInBuckets find if tag in bucket i1 i2
func (t *SingleTable) FindTagInBuckets(i1, i2 uint64, tag uint32) bool {
	var j uint64
	for j = 0; j < uint64(t.kTagsPerBucket); j++ {
		if t.ReadTag(i1, j) == tag || t.ReadTag(i2, j) == tag {
			return true
		}
	}
	return false
}

// DeleteTagFromBucket delete tag from bucket i
func (t *SingleTable) DeleteTagFromBucket(i uint64, tag uint32) bool {
	var j uint64
	for j = 0; j < uint64(t.kTagsPerBucket); j++ {
		if t.ReadTag(i, j) == tag {
			t.WriteTag(i, j, 0)
			return true
		}
	}
	return false
}

// InsertTagToBucket insert tag into bucket i
func (t *SingleTable) InsertTagToBucket(i uint64, tag uint32, kickOut bool, oldTag *uint32) bool {
	var j uint64
	for j = 0; j < uint64(t.kTagsPerBucket); j++ {
		if t.ReadTag(i, j) == 0 {
			t.WriteTag(i, j, tag)
			return true
		}
	}
	if kickOut {
		r := uint64(uint(rand.Int31()) % t.kTagsPerBucket)
		*oldTag = t.ReadTag(i, r)
		t.WriteTag(i, r, tag)
	}
	return false
}

// Reset reset table
func (t *SingleTable) Reset() {
	for i := range t.bucket {
		t.bucket[i] = 0
	}
}

// Info return table's info
func (t *SingleTable) Info() string {
	return fmt.Sprintf("SingleHashtable with tag size: %v bits \n"+
		"\t\tAssociativity: %v \n"+
		"\t\tTotal # of rows: %v\n"+
		"\t\tTotal # slots: %v\n",
		t.bitsPerTag, t.kTagsPerBucket, t.numBuckets, t.SizeInTags())
}

const (
	singleTableMetadataSize       = 3 + bytesPerUint64
	singleTableMetadataSizeLegacy = 3 + bytesPerUint32
)

// Reader returns a reader representing a TableBucket
func (t *SingleTable) Reader(legacy bool) (io.Reader, uint64) {
	var metadata []byte
	if legacy {
		metadata = make([]byte, singleTableMetadataSizeLegacy)
	} else {
		metadata = make([]byte, singleTableMetadataSize)
	}

	metadata[0] = uint8(TableTypeSingle)
	metadata[1] = uint8(t.kTagsPerBucket)
	metadata[2] = uint8(t.bitsPerTag)

	if legacy {
		binary.LittleEndian.PutUint32(metadata[3:], uint32(t.numBuckets))
	} else {
		binary.LittleEndian.PutUint64(metadata[3:], t.numBuckets)
	}

	return io.MultiReader(bytes.NewReader(metadata[:]), bytes.NewReader(t.bucket)), uint64(len(metadata)) + uint64(len(t.bucket))
}

// Decode parse a byte slice into a TableBucket
func (t *SingleTable) Decode(legacy bool, b []byte) error {
	tagsPerBucket := uint(b[1])
	bitsPerTag := uint(b[2])

	var numBuckets, offset uint64
	if legacy {
		numBuckets = uint64(binary.LittleEndian.Uint32(b[3:]))
		offset = singleTableMetadataSizeLegacy
	} else {
		numBuckets = binary.LittleEndian.Uint64(b[3:])
		offset = singleTableMetadataSize
	}

	return t.Init(tagsPerBucket, bitsPerTag, numBuckets, b[offset:])
}
