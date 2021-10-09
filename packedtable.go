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

// PackedTable using Permutation encoding to save 1 bit per tag
type PackedTable struct {
	kDirBitsPerTag  uint
	kBitsPerBucket  uint
	kBytesPerBucket uint
	kDirBitsMask    uint32
	bitsPerTag      uint

	len        uint
	numBuckets uint
	buckets    []byte
	perm       PermEncoding
}

// NewPackedTable return a packedTable
func NewPackedTable() *PackedTable {
	return &PackedTable{}
}

const (
	cFpSize       = 4
	tagsPerPTable = 4
	codeSize      = 12
)

// Init init table
func (p *PackedTable) Init(_, bitsPerTag, num uint, initialBucketsHint []byte) error {
	p.bitsPerTag = bitsPerTag
	p.numBuckets = num

	p.kDirBitsPerTag = p.bitsPerTag - cFpSize
	p.kBitsPerBucket = (p.bitsPerTag - 1) * tagsPerPTable
	p.kBytesPerBucket = (p.kBitsPerBucket + 7) >> 3
	p.kDirBitsMask = ((1 << p.kDirBitsPerTag) - 1) << cFpSize
	// NOTE: use 7 extra bytes to avoid overrun as we always read a uint64
	p.len = (p.kBitsPerBucket*p.numBuckets+7)>>3 + 7
	buckets, err := getBucketsFromHint(initialBucketsHint, p.len)
	if err != nil {
		return err
	}
	p.buckets = buckets
	p.perm.Init()
	return nil
}

// NumBuckets return num of table buckets
func (p *PackedTable) NumBuckets() uint {
	return p.numBuckets
}

// SizeInTags return num of tags that table can store
func (p *PackedTable) SizeInTags() uint {
	return tagsPerPTable * p.numBuckets
}

// SizeInBytes return bytes occupancy of table
func (p *PackedTable) SizeInBytes() uint {
	return p.len
}

// BitsPerItem return bits occupancy per item of table
func (p *PackedTable) BitsPerItem() uint {
	return p.bitsPerTag
}

// PrintBucket print a bucket
func (p *PackedTable) PrintBucket(i uint) {
	pos := p.kBitsPerBucket * i / bitsPerByte
	fmt.Printf("\tbucketBits  =%x\n", p.buckets[pos:pos+p.kBytesPerBucket])
	var tags [tagsPerPTable]uint32
	p.ReadBucket(i, &tags)
	p.PrintTags(tags)
}

// PrintTags print tags
func (p *PackedTable) PrintTags(tags [tagsPerPTable]uint32) {
	var lowBits [tagsPerPTable]uint8
	var dirBits [tagsPerPTable]uint32
	for j := 0; j < tagsPerPTable; j++ {
		lowBits[j] = uint8(tags[j] & 0x0f)
		dirBits[j] = (tags[j] & p.kDirBitsMask) >> cFpSize
	}
	codeword := p.perm.Encode(lowBits)
	fmt.Printf("\tcodeword  =%x\n", codeword)
	for j := 0; j < tagsPerPTable; j++ {
		fmt.Printf("\ttag[%v]: %x lowBits=%x dirBits=%x\n", j, tags[j], lowBits[j], dirBits[j])
	}
}

func (p *PackedTable) sortPair(a, b *uint32) {
	if (*a & 0x0f) > (*b & 0x0f) {
		*a, *b = *b, *a
	}
}

func (p *PackedTable) sortTags(tags *[tagsPerPTable]uint32) {
	p.sortPair(&tags[0], &tags[2])
	p.sortPair(&tags[1], &tags[3])
	p.sortPair(&tags[0], &tags[1])
	p.sortPair(&tags[2], &tags[3])
	p.sortPair(&tags[1], &tags[2])
}

// ReadBucket read and decode the bucket i, pass the 4 decoded tags to the 2nd arg
// bucket bits = 12 codeword bits + dir bits of tag1 + dir bits of tag2 ...
func (p *PackedTable) ReadBucket(i uint, tags *[tagsPerPTable]uint32) {
	var codeword uint16
	var lowBits [tagsPerPTable]uint8
	pos := i * p.kBitsPerBucket >> 3
	switch p.bitsPerTag {
	case 5:
		// 1 dirBits per tag, 16 bits per bucket
		bucketBits := uint16(p.buckets[pos]) | uint16(p.buckets[pos+1])<<8
		codeword = bucketBits & 0x0fff
		tags[0] = uint32(bucketBits>>8) & p.kDirBitsMask
		tags[1] = uint32(bucketBits>>9) & p.kDirBitsMask
		tags[2] = uint32(bucketBits>>10) & p.kDirBitsMask
		tags[3] = uint32(bucketBits>>11) & p.kDirBitsMask
	case 6:
		// 2 dirBits per tag, 20 bits per bucket
		bucketBits := uint32(p.buckets[pos]) | uint32(p.buckets[pos+1])<<8 | uint32(p.buckets[pos+2])<<16 | uint32(p.buckets[pos+3])<<24
		codeword = uint16(bucketBits) >> ((i & 1) << 2) & 0x0fff
		tags[0] = (bucketBits >> (8 + ((i & 1) << 2))) & p.kDirBitsMask
		tags[1] = (bucketBits >> (10 + ((i & 1) << 2))) & p.kDirBitsMask
		tags[2] = (bucketBits >> (12 + ((i & 1) << 2))) & p.kDirBitsMask
		tags[3] = (bucketBits >> (14 + ((i & 1) << 2))) & p.kDirBitsMask
	case 7:
		// 3 dirBits per tag, 24 bits per bucket
		bucketBits := uint32(p.buckets[pos]) | uint32(p.buckets[pos+1])<<8 | uint32(p.buckets[pos+2])<<16 | uint32(p.buckets[pos+3])<<24
		codeword = uint16(bucketBits) & 0x0fff
		tags[0] = (bucketBits >> 8) & p.kDirBitsMask
		tags[1] = (bucketBits >> 11) & p.kDirBitsMask
		tags[2] = (bucketBits >> 14) & p.kDirBitsMask
		tags[3] = (bucketBits >> 17) & p.kDirBitsMask
	case 8:
		// 4 dirBits per tag, 28 bits per bucket
		bucketBits := uint32(p.buckets[pos]) | uint32(p.buckets[pos+1])<<8 | uint32(p.buckets[pos+2])<<16 | uint32(p.buckets[pos+3])<<24
		codeword = uint16(bucketBits) >> ((i & 1) << 2) & 0x0fff
		tags[0] = (bucketBits >> (8 + ((i & 1) << 2))) & p.kDirBitsMask
		tags[1] = (bucketBits >> (12 + ((i & 1) << 2))) & p.kDirBitsMask
		tags[2] = (bucketBits >> (16 + ((i & 1) << 2))) & p.kDirBitsMask
		tags[3] = (bucketBits >> (20 + ((i & 1) << 2))) & p.kDirBitsMask
	case 9:
		// 5 dirBits per tag, 32 bits per bucket
		bucketBits := uint32(p.buckets[pos]) | uint32(p.buckets[pos+1])<<8 | uint32(p.buckets[pos+2])<<16 | uint32(p.buckets[pos+3])<<24
		codeword = uint16(bucketBits) & 0x0fff
		tags[0] = (bucketBits >> 8) & p.kDirBitsMask
		tags[1] = (bucketBits >> 13) & p.kDirBitsMask
		tags[2] = (bucketBits >> 18) & p.kDirBitsMask
		tags[3] = (bucketBits >> 23) & p.kDirBitsMask
	case 13:
		// 9 dirBits per tag,  48 bits per bucket
		bucketBits := uint64(p.buckets[pos]) | uint64(p.buckets[pos+1])<<8 | uint64(p.buckets[pos+2])<<16 | uint64(p.buckets[pos+3])<<24 |
			uint64(p.buckets[pos+4])<<32 | uint64(p.buckets[pos+5])<<40 | uint64(p.buckets[pos+6])<<48 | uint64(p.buckets[pos+7])<<56
		codeword = uint16(bucketBits) & 0x0fff
		tags[0] = uint32((bucketBits)>>8) & p.kDirBitsMask
		tags[1] = uint32((bucketBits)>>17) & p.kDirBitsMask
		tags[2] = uint32((bucketBits)>>26) & p.kDirBitsMask
		tags[3] = uint32((bucketBits)>>35) & p.kDirBitsMask
	case 17:
		// 13 dirBits per tag, 64 bits per bucket
		bucketBits := uint64(p.buckets[pos]) | uint64(p.buckets[pos+1])<<8 | uint64(p.buckets[pos+2])<<16 | uint64(p.buckets[pos+3])<<24 |
			uint64(p.buckets[pos+4])<<32 | uint64(p.buckets[pos+5])<<40 | uint64(p.buckets[pos+6])<<48 | uint64(p.buckets[pos+7])<<56
		codeword = uint16(bucketBits) & 0x0fff
		tags[0] = uint32((bucketBits)>>8) & p.kDirBitsMask
		tags[1] = uint32((bucketBits)>>21) & p.kDirBitsMask
		tags[2] = uint32((bucketBits)>>34) & p.kDirBitsMask
		tags[3] = uint32((bucketBits)>>47) & p.kDirBitsMask
	default:
		u1, u2, rShift := p.readOutBytes(i, pos)
		codeword = uint16(u1>>rShift) & 0x0fff
		for k := 0; k < tagsPerPTable; k++ {
			tags[k] = uint32(u1 >> rShift >> (codeSize - cFpSize + k*int(p.kDirBitsPerTag)))
			shift := codeSize - cFpSize + k*int(p.kDirBitsPerTag) - 64 + int(rShift)
			if shift < 0 {
				tags[k] |= uint32(u2 << -shift)
			} else {
				tags[k] |= uint32(u2 >> shift)
			}
			tags[k] &= p.kDirBitsMask
		}
	}

	/* codeword is the lowest 12 bits in the bucket */
	p.perm.Decode(codeword, &lowBits)

	tags[0] |= uint32(lowBits[0])
	tags[1] |= uint32(lowBits[1])
	tags[2] |= uint32(lowBits[2])
	tags[3] |= uint32(lowBits[3])
}

func (p *PackedTable) readOutBytes(i, pos uint) (uint64, uint64, uint) {
	rShift := (p.kBitsPerBucket * i) & (bitsPerByte - 1)
	// tag is max 32bit, store 31bit per tag, so max occupies 16 bytes
	kBytes := (rShift + p.kBitsPerBucket + 7) / bitsPerByte

	var u1, u2 uint64
	for k := uint(0); k < kBytes; k++ {
		if k < bytesPerUint64 {
			u1 |= uint64(p.buckets[pos+k]) << (k * bitsPerByte)
		} else {
			u2 |= uint64(p.buckets[pos+k]) << ((k - bytesPerUint64) * bitsPerByte)
		}
	}

	return u1, u2, rShift
}

// WriteBucket write tags into bucket i
func (p *PackedTable) WriteBucket(i uint, tags [tagsPerPTable]uint32) {
	p.sortTags(&tags)

	/* put in direct bits for each tag*/
	var lowBits [tagsPerPTable]uint8
	var highBits [tagsPerPTable]uint32

	lowBits[0] = uint8(tags[0] & 0x0f)
	lowBits[1] = uint8(tags[1] & 0x0f)
	lowBits[2] = uint8(tags[2] & 0x0f)
	lowBits[3] = uint8(tags[3] & 0x0f)

	highBits[0] = tags[0] & 0xfffffff0
	highBits[1] = tags[1] & 0xfffffff0
	highBits[2] = tags[2] & 0xfffffff0
	highBits[3] = tags[3] & 0xfffffff0
	// note that :  tags[j] = lowBits[j] | highBits[j]

	codeword := p.perm.Encode(lowBits)
	pos := i * p.kBitsPerBucket >> 3
	switch p.kBitsPerBucket {
	case 16:
		// 1 dirBits per tag
		v := codeword | uint16(highBits[0]<<8) | uint16(highBits[1]<<9) |
			uint16(highBits[2]<<10) | uint16(highBits[3]<<11)
		p.buckets[pos] = byte(v)
		p.buckets[pos+1] = byte(v >> 8)
	case 20:
		// 2 dirBits per tag
		var v uint32
		v = uint32(p.buckets[pos]) | uint32(p.buckets[pos+1])<<8 | uint32(p.buckets[pos+2])<<16 | uint32(p.buckets[pos+3])<<24
		if (i & 0x0001) == 0 {
			v &= 0xfff00000
			v |= uint32(codeword) | (highBits[0] << 8) |
				(highBits[1] << 10) | (highBits[2] << 12) |
				(highBits[3] << 14)
		} else {
			v &= 0xff00000f
			v |= uint32(codeword)<<4 | (highBits[0] << 12) |
				(highBits[1] << 14) | (highBits[2] << 16) |
				(highBits[3] << 18)
		}
		p.buckets[pos] = byte(v)
		p.buckets[pos+1] = byte(v >> 8)
		p.buckets[pos+2] = byte(v >> 16)
		p.buckets[pos+3] = byte(v >> 24)
	case 24:
		// 3 dirBits per tag
		var v uint32
		v = uint32(p.buckets[pos]) | uint32(p.buckets[pos+1])<<8 | uint32(p.buckets[pos+2])<<16 | uint32(p.buckets[pos+3])<<24
		v &= 0xff000000
		v |= uint32(codeword) | (highBits[0] << 8) | (highBits[1] << 11) |
			(highBits[2] << 14) | (highBits[3] << 17)
		p.buckets[pos] = byte(v)
		p.buckets[pos+1] = byte(v >> 8)
		p.buckets[pos+2] = byte(v >> 16)
		p.buckets[pos+3] = byte(v >> 24)
	case 28:
		// 4 dirBits per tag
		var v uint32
		v = uint32(p.buckets[pos]) | uint32(p.buckets[pos+1])<<8 | uint32(p.buckets[pos+2])<<16 | uint32(p.buckets[pos+3])<<24
		if (i & 0x0001) == 0 {
			v &= 0xf0000000
			v |= uint32(codeword) | (highBits[0] << 8) |
				(highBits[1] << 12) | (highBits[2] << 16) |
				(highBits[3] << 20)
		} else {
			v &= 0x0000000f
			v |= uint32(codeword)<<4 | (highBits[0] << 12) |
				(highBits[1] << 16) | (highBits[2] << 20) |
				(highBits[3] << 24)
		}
		p.buckets[pos] = byte(v)
		p.buckets[pos+1] = byte(v >> 8)
		p.buckets[pos+2] = byte(v >> 16)
		p.buckets[pos+3] = byte(v >> 24)
	case 32:
		// 5 dirBits per tag
		v := uint32(codeword) | (highBits[0] << 8) | (highBits[1] << 13) |
			(highBits[2] << 18) | (highBits[3] << 23)
		p.buckets[pos] = byte(v)
		p.buckets[pos+1] = byte(v >> 8)
		p.buckets[pos+2] = byte(v >> 16)
		p.buckets[pos+3] = byte(v >> 24)
	case 48:
		// 9 dirBits per tag
		var v uint64
		v = uint64(p.buckets[pos]) | uint64(p.buckets[pos+1])<<8 | uint64(p.buckets[pos+2])<<16 | uint64(p.buckets[pos+3])<<24 |
			uint64(p.buckets[pos+4])<<32 | uint64(p.buckets[pos+5])<<40 | uint64(p.buckets[pos+6])<<48 | uint64(p.buckets[pos+7])<<56
		v &= 0xffff000000000000
		v |= uint64(codeword) | uint64(highBits[0])<<8 |
			uint64(highBits[1])<<17 | uint64(highBits[2])<<26 |
			uint64(highBits[3])<<35
		p.buckets[pos] = byte(v)
		p.buckets[pos+1] = byte(v >> 8)
		p.buckets[pos+2] = byte(v >> 16)
		p.buckets[pos+3] = byte(v >> 24)
		p.buckets[pos+4] = byte(v >> 32)
		p.buckets[pos+5] = byte(v >> 40)
		p.buckets[pos+6] = byte(v >> 48)
		p.buckets[pos+7] = byte(v >> 56)
	case 64:
		// 13 dirBits per tag
		v := uint64(codeword) | uint64(highBits[0])<<8 |
			uint64(highBits[1])<<21 | uint64(highBits[2])<<34 |
			uint64(highBits[3])<<47
		p.buckets[pos] = byte(v)
		p.buckets[pos+1] = byte(v >> 8)
		p.buckets[pos+2] = byte(v >> 16)
		p.buckets[pos+3] = byte(v >> 24)
		p.buckets[pos+4] = byte(v >> 32)
		p.buckets[pos+5] = byte(v >> 40)
		p.buckets[pos+6] = byte(v >> 48)
		p.buckets[pos+7] = byte(v >> 56)
	default:
		p.writeInBytes(i, pos, codeword, highBits)
	}
}

func (p *PackedTable) writeInBytes(i, pos uint, codeword uint16, highBits [tagsPerPTable]uint32) {
	rShift := (p.kBitsPerBucket * i) & (bitsPerByte - 1)
	lShift := (rShift + p.kBitsPerBucket) & (bitsPerByte - 1)
	// tag is max 32bit, store 31bit per tag, so max occupies 16 bytes
	kBytes := (rShift + p.kBitsPerBucket + 7) / bitsPerByte

	rMask := uint8(0xff) >> (bitsPerByte - rShift)
	lMask := uint8(0xff) << lShift
	if lShift == 0 {
		lMask = uint8(0)
	}

	var u1, u2 uint64
	u1 |= uint64(p.buckets[pos] & rMask)
	end := kBytes - 1
	if kBytes > bytesPerUint64 {
		u2 |= uint64(p.buckets[pos+end]&lMask) << ((end - bytesPerUint64) * bitsPerByte)
	} else {
		u1 |= uint64(p.buckets[pos+end]&lMask) << (end * bitsPerByte)
	}

	u1 |= uint64(codeword) << rShift
	for k := 0; k < tagsPerPTable; k++ {
		u1 |= uint64(highBits[k]) << (codeSize - cFpSize + k*int(p.kDirBitsPerTag)) << rShift
		shift := codeSize - cFpSize + k*int(p.kDirBitsPerTag) - 64 + int(rShift)
		if shift < 0 {
			u2 |= uint64(highBits[k]) >> -shift
		} else {
			u2 |= uint64(highBits[k]) << shift
		}
	}

	for k := uint(0); k < kBytes; k++ {
		if k < bytesPerUint64 {
			p.buckets[pos+k] = byte(u1 >> (k * bitsPerByte))
		} else {
			p.buckets[pos+k] = byte(u2 >> ((k - bytesPerUint64) * bitsPerByte))
		}
	}

	return
}

// FindTagInBuckets find if tag in bucket i1 i2
func (p *PackedTable) FindTagInBuckets(i1, i2 uint, tag uint32) bool {
	var tags1, tags2 [tagsPerPTable]uint32
	p.ReadBucket(i1, &tags1)
	p.ReadBucket(i2, &tags2)

	return (tags1[0] == tag) || (tags1[1] == tag) || (tags1[2] == tag) ||
		(tags1[3] == tag) || (tags2[0] == tag) || (tags2[1] == tag) ||
		(tags2[2] == tag) || (tags2[3] == tag)
}

// DeleteTagFromBucket delete tag from bucket i
func (p *PackedTable) DeleteTagFromBucket(i uint, tag uint32) bool {
	var tags [tagsPerPTable]uint32
	p.ReadBucket(i, &tags)
	for j := 0; j < tagsPerPTable; j++ {
		if tags[j] == tag {
			tags[j] = 0
			p.WriteBucket(i, tags)
			return true
		}
	}
	return false
}

// InsertTagToBucket insert tag into bucket i
func (p *PackedTable) InsertTagToBucket(i uint, tag uint32, kickOut bool, oldTag *uint32) bool {
	var tags [tagsPerPTable]uint32
	p.ReadBucket(i, &tags)
	for j := 0; j < tagsPerPTable; j++ {
		if tags[j] == 0 {
			tags[j] = tag
			p.WriteBucket(i, tags)
			return true
		}
	}
	if kickOut {
		r := uint(rand.Int31()) & 3
		*oldTag = tags[r]
		tags[r] = tag
		p.WriteBucket(i, tags)
	}
	return false
}

// Reset reset table
func (p *PackedTable) Reset() {
	for i := range p.buckets {
		p.buckets[i] = 0
	}
}

// Info return table's info
func (p *PackedTable) Info() string {
	return fmt.Sprintf("PackedHashtable with tag size: %v bits \n"+
		"\t\t4 packed bits(3 bits after compression) and %v direct bits\n"+
		"\t\tAssociativity: 4 \n"+
		"\t\tTotal # of rows: %v\n"+
		"\t\tTotal # slots: %v\n",
		p.bitsPerTag, p.kDirBitsPerTag, p.numBuckets, p.SizeInTags())
}

const packedTableMetadataSize = 2+bytesPerUint32

// Encode returns a byte slice representing a TableBucket
func (p *PackedTable) Reader() (io.Reader, uint) {
	var metadata [packedTableMetadataSize]byte
	metadata[0] = uint8(TableTypePacked)
	metadata[1] = uint8(p.bitsPerTag)
	binary.LittleEndian.PutUint32(metadata[2:], uint32(p.numBuckets))
	return io.MultiReader(bytes.NewReader(metadata[:]), bytes.NewReader(p.buckets)), uint(len(metadata) + len(p.buckets))
}

// Decode parse a byte slice into a TableBucket
func (p *PackedTable) Decode(b []byte) error {
	bitsPerTag := uint(b[1])
	numBuckets := uint(binary.LittleEndian.Uint32(b[2:]))
	return p.Init(0, bitsPerTag, numBuckets, b[6:])
}
