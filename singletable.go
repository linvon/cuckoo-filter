/*
 * Copyright (C) linvon
 * Date  2021/2/18 10:29
 */

package cuckoo

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
)

//SingleTable the most naive table implementation: one huge bit array
type SingleTable struct {
	kTagsPerBucket uint
	numBuckets     uint
	bitsPerTag     uint
	tagMask        uint32
	bucket         []byte
	len            uint
}

//NewSingleTable return a singleTable
func NewSingleTable() *SingleTable {
	return &SingleTable{}
}

//Init init table
func (t *SingleTable) Init(tagsPerBucket, bitsPerTag, num uint) {
	t.bitsPerTag = bitsPerTag
	t.numBuckets = num
	t.kTagsPerBucket = tagsPerBucket

	t.tagMask = (1 << bitsPerTag) - 1
	t.len = (t.bitsPerTag*t.kTagsPerBucket*t.numBuckets + 7) >> 3

	t.bucket = make([]byte, t.len)
}

//NumBuckets return num of table buckets
func (t *SingleTable) NumBuckets() uint {
	return t.numBuckets
}

//SizeInBytes return bytes occupancy of table
func (t *SingleTable) SizeInBytes() uint {
	return t.len
}

//SizeInTags return num of tags that table can store
func (t *SingleTable) SizeInTags() uint {
	return t.kTagsPerBucket * t.numBuckets
}

//BitsPerItem return bits occupancy per item of table
func (t *SingleTable) BitsPerItem() uint {
	return t.bitsPerTag
}

//ReadTag read tag from bucket(i,j)
func (t *SingleTable) ReadTag(i, j uint) uint32 {
	pos := int(i*t.bitsPerTag*t.kTagsPerBucket+t.bitsPerTag*j) / bitsPerByte
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
		tag = uint32(binary.LittleEndian.Uint16([]byte{t.bucket[pos], t.bucket[pos+1]})) >> ((j & 1) << 2)
	case 16:
		tag = uint32(binary.LittleEndian.Uint16([]byte{t.bucket[pos], t.bucket[pos+1]}))
	case 32:
		tag = binary.LittleEndian.Uint32([]byte{t.bucket[pos], t.bucket[pos+1], t.bucket[pos+2], t.bucket[pos+3]})
	default:
		tmp := t.readOutUint64(i, j, pos)
		tag = uint32(tmp)
	}
	return tag & t.tagMask
}

func (t *SingleTable) readOutUint64(i, j uint, pos int) uint64 {
	rShift := (i*t.bitsPerTag*t.kTagsPerBucket + t.bitsPerTag*j) & (bitsPerByte - 1)
	kBytes := int((rShift + t.bitsPerTag + 7) / bitsPerByte)
	// tag is max 32bit, so max occupies 5 bytes
	b := make([]byte, bytesPerUint64)
	for k := 0; k < bytesPerUint64; k++ {
		if k+1 <= kBytes {
			b[k] = t.bucket[pos+k]
		} else {
			b[k] = 0
		}
	}
	tmp := binary.LittleEndian.Uint64(b)
	tmp >>= rShift
	return tmp
}

//WriteTag write tag into bucket(i,j)
func (t *SingleTable) WriteTag(i, j uint, n uint32) {
	pos := int(i*t.bitsPerTag*t.kTagsPerBucket+t.bitsPerTag*j) / bitsPerByte
	var tag = n & t.tagMask
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
		b := make([]byte, 2)
		tmp = binary.LittleEndian.Uint16([]byte{t.bucket[pos], t.bucket[pos+1]})
		if (j & 1) == 0 {
			tmp &= 0xf000
			tmp |= uint16(tag)
		} else {
			tmp &= 0x000f
			tmp |= uint16(tag) << 4
		}
		binary.LittleEndian.PutUint16(b, tmp)
		t.bucket[pos] = b[0]
		t.bucket[pos+1] = b[1]
	case 16:
		b := make([]byte, 2)
		binary.LittleEndian.PutUint16(b, uint16(tag))
		t.bucket[pos] = b[0]
		t.bucket[pos+1] = b[1]
	case 32:
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, tag)
		t.bucket[pos] = b[0]
		t.bucket[pos+1] = b[1]
		t.bucket[pos+2] = b[2]
		t.bucket[pos+3] = b[3]
	default:
		b := t.writeInByte(i, j, pos, tag)
		for k := 0; k < bytesPerUint64; k++ {
			if pos+k >= int(t.len) {
				break
			} else {
				t.bucket[pos+k] = b[k]
			}
		}
	}
}

func (t *SingleTable) writeInByte(i, j uint, pos int, tag uint32) []byte {
	rShift := (i*t.bitsPerTag*t.kTagsPerBucket + t.bitsPerTag*j) & (bitsPerByte - 1)
	kBytes := int((rShift + t.bitsPerTag + 7) / bitsPerByte)
	lShift := (rShift + t.bitsPerTag) & (bitsPerByte - 1)
	// tag is max 32bit, so max occupies 5 bytes
	b := make([]byte, bytesPerUint64)
	for k := 0; k < bytesPerUint64; k++ {
		if pos+k >= int(t.len) {
			b[k] = 0
		} else {
			b[k] = t.bucket[pos+k]
		}
	}
	rMask := uint8(0xff) >> (bitsPerByte - rShift)
	lMask := uint8(0xff) << lShift
	if lShift == 0 {
		lMask = uint8(0)
	}
	if kBytes == 1 {
		mask := lMask | rMask
		b[0] &= mask
	} else {
		b[0] &= rMask
		for k := 1; k < kBytes-1; k++ {
			b[k] = 0
		}
		b[kBytes-1] &= lMask
	}
	tmp := binary.LittleEndian.Uint64(b)
	tmp |= uint64(tag) << rShift
	binary.LittleEndian.PutUint64(b, tmp)
	return b
}

//FindTagInBuckets find if tag in bucket i1 i2
func (t *SingleTable) FindTagInBuckets(i1, i2 uint, tag uint32) bool {
	var j uint
	for j = 0; j < t.kTagsPerBucket; j++ {
		if t.ReadTag(i1, j) == tag || t.ReadTag(i2, j) == tag {
			return true
		}
	}
	return false
}

//DeleteTagFromBucket delete tag from bucket i
func (t *SingleTable) DeleteTagFromBucket(i uint, tag uint32) bool {
	var j uint
	for j = 0; j < t.kTagsPerBucket; j++ {
		if t.ReadTag(i, j) == tag {
			t.WriteTag(i, j, 0)
			return true
		}
	}
	return false
}

//InsertTagToBucket insert tag into bucket i
func (t *SingleTable) InsertTagToBucket(i uint, tag uint32, kickOut bool, oldTag *uint32) bool {
	var j uint
	for j = 0; j < t.kTagsPerBucket; j++ {
		if t.ReadTag(i, j) == 0 {
			t.WriteTag(i, j, tag)
			return true
		}
	}
	if kickOut {
		r := uint(rand.Int31()) % t.kTagsPerBucket
		*oldTag = t.ReadTag(i, r)
		t.WriteTag(i, r, tag)
	}
	return false
}

//Reset reset table
func (t *SingleTable) Reset() {
	for i := range t.bucket {
		t.bucket[i] = 0
	}
}

//Info return table's info
func (t *SingleTable) Info() string {
	return fmt.Sprintf("SingleHashtable with tag size: %v bits \n"+
		"\t\tAssociativity: %v \n"+
		"\t\tTotal # of rows: %v\n"+
		"\t\tTotal # slots: %v\n",
		t.bitsPerTag, t.kTagsPerBucket, t.numBuckets, t.SizeInTags())
}

// Encode returns a byte slice representing a TableBucket
func (t *SingleTable) Encode() []byte {
	bytes := make([]byte, t.len+7)
	bytes[0] = uint8(TableTypeSingle)
	bytes[1] = uint8(t.kTagsPerBucket)
	bytes[2] = uint8(t.bitsPerTag)
	b := make([]byte, bytesPerUint32)
	binary.LittleEndian.PutUint32(b, uint32(t.numBuckets))
	copy(bytes[3:], b)
	copy(bytes[7:], t.bucket)
	return bytes
}

// Decode parse a byte slice into a TableBucket
func (t *SingleTable) Decode(bytes []byte) error {
	tagsPerBucket := uint(bytes[1])
	bitsPerTag := uint(bytes[2])
	numBuckets := uint(binary.LittleEndian.Uint32(bytes[3:7]))
	t.Init(tagsPerBucket, bitsPerTag, numBuckets)
	if len(bytes) != int(t.len+7) {
		return errors.New("unexpected bytes length")
	}
	copy(t.bucket, bytes[7:])
	return nil
}
