/*
 * Copyright (C) linvon
 * Date  2021/2/18 10:29
 */

package cuckoo

import (
	"encoding/binary"
	"fmt"
	"math/rand"
)

// the most naive table implementation: one huge bit array
type SingleTable struct {
	kBytesPerBucket uint
	//kPaddingBuckets uint
	kTagsPerBucket uint
	tagMask        uint32

	numBuckets uint
	bitsPerTag uint
	buckets    []bucket
}

type bucket struct {
	bits []byte
}

func NewSingleTable() *SingleTable {
	return &SingleTable{}
}

func (t *SingleTable) Init(tagsPerBucket, bitsPerTag, num uint) {
	t.bitsPerTag = bitsPerTag
	t.numBuckets = num
	t.kTagsPerBucket = tagsPerBucket

	t.kBytesPerBucket = (bitsPerTag*t.kTagsPerBucket + 7) >> 3
	t.tagMask = (1 << bitsPerTag) - 1
	//// NOTE: accommodate extra buckets if necessary to avoid overrun as we always read a uint64
	//t.kPaddingBuckets = ((((t.kBytesPerBucket + 7) / 8) * 8) - 1) / t.kBytesPerBucket

	t.buckets = make([]bucket, t.numBuckets)
	for i := range t.buckets {
		t.buckets[i] = bucket{bits: make([]byte, t.kBytesPerBucket, t.kBytesPerBucket)}
	}
}

func (t *SingleTable) NumBuckets() uint {
	return t.numBuckets
}

func (t *SingleTable) SizeInBytes() uint {
	return t.kBytesPerBucket * t.numBuckets
}

func (t *SingleTable) SizeInTags() uint {
	return t.kTagsPerBucket * t.numBuckets
}

// read tag from pos(i,j)
func (t *SingleTable) ReadTag(i, j uint) uint32 {
	p := t.buckets[i]
	var tag uint32
	/* following code only works for little-endian */
	switch t.bitsPerTag {
	case 2:
		pos := j / 4
		shift := j % 4
		tag = uint32(p.bits[pos]) >> (2 * shift)
	case 4:
		pos := j >> 1
		tag = uint32(p.bits[pos]) >> ((j & 1) << 2)
	case 8:
		tag = uint32(p.bits[j])
	case 12:
		pos := j + (j >> 1)
		tag = uint32(binary.LittleEndian.Uint16([]byte{p.bits[pos], p.bits[pos+1]})) >> ((j & 1) << 2)
	case 16:
		pos := j << 1
		tag = uint32(binary.LittleEndian.Uint16([]byte{p.bits[pos], p.bits[pos+1]}))
	case 32:
		pos := j << 2
		tag = binary.LittleEndian.Uint32([]byte{p.bits[pos], p.bits[pos+1], p.bits[pos+2], p.bits[pos+3]})
	default:
		pos := int(t.bitsPerTag * j / 8)
		rShift := t.bitsPerTag * j % 8
		kBytes := int((rShift + t.bitsPerTag + 7) / 8)
		// tag is max 32bit, so max occupies 5 bytes
		b := make([]byte, 8)
		for k := 0; k < 8; k++ {
			if k+1 <= kBytes {
				b[k] = p.bits[pos+k]
			} else {
				b[k] = 0
			}
		}

		tmp := binary.LittleEndian.Uint64(b)
		tmp >>= rShift

		tag = uint32(tmp)
	}
	return tag & t.tagMask
}

// write tag to pos(i,j)
func (t *SingleTable) WriteTag(i, j uint, n uint32) {
	p := t.buckets[i]
	var tag = n & t.tagMask
	/* following code only works for little-endian */
	switch t.bitsPerTag {
	case 2:
		pos := j / 4
		shift := j % 4
		p.bits[pos] &= ^(0x03 << (2 * shift))
		p.bits[pos] |= uint8(tag) << (2 * shift)
	case 4:
		pos := j >> 1
		if (j & 1) == 0 {
			p.bits[pos] &= 0xf0
			p.bits[pos] |= uint8(tag)
		} else {
			p.bits[pos] &= 0x0f
			p.bits[pos] |= uint8(tag) << 4
		}
	case 8:
		p.bits[j] = uint8(tag)
	case 12:
		pos := j + (j >> 1)
		var tmp uint16
		b := make([]byte, 2)
		tmp = binary.LittleEndian.Uint16([]byte{p.bits[pos], p.bits[pos+1]})
		if (j & 1) == 0 {
			tmp &= 0xf000
			tmp |= uint16(tag)
		} else {
			tmp &= 0x000f
			tmp |= uint16(tag) << 4
		}
		binary.LittleEndian.PutUint16(b, tmp)
		p.bits[pos] = b[0]
		p.bits[pos+1] = b[1]
	case 16:
		pos := j << 1
		b := make([]byte, 2)
		binary.LittleEndian.PutUint16(b, uint16(tag))
		p.bits[pos] = b[0]
		p.bits[pos+1] = b[1]
	case 32:
		pos := j << 2
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, tag)
		p.bits[pos] = b[0]
		p.bits[pos+1] = b[1]
		p.bits[pos+2] = b[2]
		p.bits[pos+3] = b[3]
	default:
		pos := int(t.bitsPerTag * j / 8)
		rShift := t.bitsPerTag * j % 8
		kBytes := int((rShift + t.bitsPerTag + 7) / 8)
		lShift := (rShift + t.bitsPerTag) % 8
		// tag is max 32bit, so max occupies 5 bytes
		b := make([]byte, 8)
		for k := 0; k < 8; k++ {
			if pos +k >= len(p.bits) {
				b[k] = 0
			} else {
				b[k] = p.bits[pos+k]
			}
		}
		rMask := uint8(0xff) >> (8 - rShift)
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

		for k := 0; k < 8; k++ {
			if pos + k >= len(p.bits) {
				break
			} else {
				p.bits[pos+k] = b[k]
			}
		}
	}
}

func (t *SingleTable) FindTagInBuckets(i1, i2 uint, tag uint32) bool {
	var j uint
	for j = 0; j < t.kTagsPerBucket; j++ {
		if t.ReadTag(i1, j) == tag || t.ReadTag(i2, j) == tag {
			return true
		}
	}
	return false
}

func (t *SingleTable) FindTagInBucket(i uint, tag uint32) bool {
	var j uint
	for j = 0; j < t.kTagsPerBucket; j++ {
		if t.ReadTag(i, j) == tag {
			return true
		}
	}
	return false
}

func (t *SingleTable) DeleteTagFromBucket(i uint, tag uint32) bool {
	var j uint
	for j = 0; j < t.kTagsPerBucket; j++ {
		if t.ReadTag(i, j) == tag {
			if t.FindTagInBucket(i, tag) != true {
				panic("not exist")
			}
			t.WriteTag(i, j, 0)
			return true
		}
	}
	return false
}

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

func (t *SingleTable) NumTagsInBucket(i uint) uint {
	var j, num uint
	for j = 0; j < t.kTagsPerBucket; j++ {
		if t.ReadTag(i, j) != 0 {
			num++
		}
	}
	return num
}

func (t *SingleTable) Info() string {
	return fmt.Sprintf("SingleHashtable with tag size: %v bits \n"+
		"\t\tAssociativity: %v \n"+
		"\t\tTotal # of rows: %v\n"+
		"\t\tTotal # slots: %v\n",
		t.bitsPerTag, t.kTagsPerBucket, t.numBuckets, t.SizeInTags())
}
