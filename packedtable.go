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

// Using Permutation encoding to save 1 bit per tag
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

func NewPackedTable() *PackedTable {
	return &PackedTable{}
}

func (p *PackedTable) Init(tagsPerBucket, bitsPerTag, num uint) {
	p.bitsPerTag = bitsPerTag
	p.numBuckets = num

	p.kDirBitsPerTag = p.bitsPerTag - 4
	p.kBitsPerBucket = (3 + p.kDirBitsPerTag) * 4
	p.kBytesPerBucket = (p.kBitsPerBucket + 7) >> 3
	p.kDirBitsMask = ((1 << p.kDirBitsPerTag) - 1) << 4
	// NOTE: use 7 extra bytes to avoid overrun as we always read a uint64
	p.len = p.kBytesPerBucket*p.numBuckets + 7
	p.buckets = make([]byte, p.len)
	p.perm.Init()
}

func (p *PackedTable) NumBuckets() uint {
	return p.numBuckets
}

func (p *PackedTable) SizeInTags() uint {
	return 4 * p.numBuckets
}

func (p *PackedTable) SizeInBytes() uint {
	return p.len
}

func (p *PackedTable) PrintBucket(i uint) {
	pos := p.kBitsPerBucket * i / 8
	fmt.Printf("\tbucketBits  =%x\n", p.buckets[pos:pos+p.kBytesPerBucket])
	var tags [4]uint32
	p.ReadBucket(i, &tags)
	p.PrintTags(tags)
}

func (p *PackedTable) PrintTags(tags [4]uint32) {
	var lowBits [4]uint8
	var dirBits [4]uint32
	for j := 0; j < 4; j++ {
		lowBits[j] = uint8(tags[j] & 0x0f)
		dirBits[j] = (tags[j] & p.kDirBitsMask) >> 4
	}
	codeword := p.perm.Encode(lowBits)
	fmt.Printf("\tcodeword  =%x\n", codeword)
	for j := 0; j < 4; j++ {
		fmt.Printf("\ttag[%v]: %x lowBits=%x dirBits=%x\n", j, tags[j], lowBits[j], dirBits[j])
	}
}

func (p *PackedTable) sortPair(a, b *uint32) {
	if (*a & 0x0f) > (*b & 0x0f) {
		*a, *b = *b, *a
	}
}

func (p *PackedTable) sortTags(tags *[4]uint32) {
	p.sortPair(&tags[0], &tags[2])
	p.sortPair(&tags[1], &tags[3])
	p.sortPair(&tags[0], &tags[1])
	p.sortPair(&tags[2], &tags[3])
	p.sortPair(&tags[1], &tags[2])
}

/* read and decode the bucket i, pass the 4 decoded tags to the 2nd arg
 * bucket bits = 12 codeword bits + dir bits of tag1 + dir bits of tag2 ...
 */
func (p *PackedTable) ReadBucket(i uint, tags *[4]uint32) {
	var codeword uint16
	var lowBits [4]uint8
	pos := i * p.kBitsPerBucket >> 3
	switch p.bitsPerTag {
	case 5:
		// 1 dirBits per tag, 16 bits per bucket
		bucketBits := binary.LittleEndian.Uint16([]byte{p.buckets[pos], p.buckets[pos+1]})
		codeword = bucketBits & 0x0fff
		tags[0] = uint32(bucketBits>>8) & p.kDirBitsMask
		tags[1] = uint32(bucketBits>>9) & p.kDirBitsMask
		tags[2] = uint32(bucketBits>>10) & p.kDirBitsMask
		tags[3] = uint32(bucketBits>>11) & p.kDirBitsMask
	case 6:
		// 2 dirBits per tag, 20 bits per bucket
		bucketBits := binary.LittleEndian.Uint32([]byte{p.buckets[pos], p.buckets[pos+1], p.buckets[pos+2], p.buckets[pos+3]})
		codeword = uint16(bucketBits) >> (0) & 0x0fff
		tags[0] = (bucketBits >> (8 + (0))) & p.kDirBitsMask
		tags[1] = (bucketBits >> (10 + 0)) & p.kDirBitsMask
		tags[2] = (bucketBits >> (12 + 0)) & p.kDirBitsMask
		tags[3] = (bucketBits >> (14 + 0)) & p.kDirBitsMask
	case 7:
		// 3 dirBits per tag, 24 bits per bucket
		bucketBits := binary.LittleEndian.Uint32([]byte{p.buckets[pos], p.buckets[pos+1], p.buckets[pos+2], p.buckets[pos+3]})
		codeword = uint16(bucketBits) & 0x0fff
		tags[0] = (bucketBits >> 8) & p.kDirBitsMask
		tags[1] = (bucketBits >> 11) & p.kDirBitsMask
		tags[2] = (bucketBits >> 14) & p.kDirBitsMask
		tags[3] = (bucketBits >> 17) & p.kDirBitsMask
	case 8:
		// 4 dirBits per tag, 28 bits per bucket
		bucketBits := binary.LittleEndian.Uint32([]byte{p.buckets[pos], p.buckets[pos+1], p.buckets[pos+2], p.buckets[pos+3]})
		codeword = uint16(bucketBits) >> ((i & 1) << 2) & 0x0fff
		tags[0] = (bucketBits >> (8 + ((i & 1) << 2))) & p.kDirBitsMask
		tags[1] = (bucketBits >> (12 + ((i & 1) << 2))) & p.kDirBitsMask
		tags[2] = (bucketBits >> (16 + ((i & 1) << 2))) & p.kDirBitsMask
		tags[3] = (bucketBits >> (20 + ((i & 1) << 2))) & p.kDirBitsMask
	case 9:
		// 5 dirBits per tag, 32 bits per bucket
		bucketBits := binary.LittleEndian.Uint32([]byte{p.buckets[pos], p.buckets[pos+1], p.buckets[pos+2], p.buckets[pos+3]})
		codeword = uint16(bucketBits) & 0x0fff
		tags[0] = (bucketBits >> 8) & p.kDirBitsMask
		tags[1] = (bucketBits >> 13) & p.kDirBitsMask
		tags[2] = (bucketBits >> 18) & p.kDirBitsMask
		tags[3] = (bucketBits >> 23) & p.kDirBitsMask
	case 13:
		// 9 dirBits per tag,  48 bits per bucket
		bucketBits := binary.LittleEndian.Uint64([]byte{p.buckets[pos], p.buckets[pos+1], p.buckets[pos+2], p.buckets[pos+3],
			p.buckets[pos+4], p.buckets[pos+5], p.buckets[pos+6], p.buckets[pos+7]})
		codeword = uint16(bucketBits) & 0x0fff
		tags[0] = uint32((bucketBits)>>8) & p.kDirBitsMask
		tags[1] = uint32((bucketBits)>>17) & p.kDirBitsMask
		tags[2] = uint32((bucketBits)>>26) & p.kDirBitsMask
		tags[3] = uint32((bucketBits)>>35) & p.kDirBitsMask
	case 17:
		// 13 dirBits per tag, 64 bits per bucket
		bucketBits := binary.LittleEndian.Uint64([]byte{p.buckets[pos], p.buckets[pos+1], p.buckets[pos+2], p.buckets[pos+3],
			p.buckets[pos+4], p.buckets[pos+5], p.buckets[pos+6], p.buckets[pos+7]})
		codeword = uint16(bucketBits) & 0x0fff
		tags[0] = uint32((bucketBits)>>8) & p.kDirBitsMask
		tags[1] = uint32((bucketBits)>>21) & p.kDirBitsMask
		tags[2] = uint32((bucketBits)>>34) & p.kDirBitsMask
		tags[3] = uint32((bucketBits)>>47) & p.kDirBitsMask
	default:
		b1 := make([]byte, 8)
		b2 := make([]byte, 8)
		for k := uint(0); k < 8; k++ {
			if k+1 <= p.kBytesPerBucket {
				b1[k] = p.buckets[pos+k]
			} else {
				b1[k] = 0
			}
		}
		if p.kBytesPerBucket > 8 {
			for k := uint(0); k < 8; k++ {
				if k+1 <= p.kBytesPerBucket-8 {
					b2[k] = p.buckets[pos+8+k]
				} else {
					b2[k] = 0
				}
			}
		}
		u1 := binary.LittleEndian.Uint64(b1)
		//u2 := binary.LittleEndian.Uint64(b2)
		codeword = uint16(u1) & 0x0fff
		for k := 0; k < 4; k++ {
			tags[k] = uint32((u1)>>(12-4+k*int(p.kDirBitsPerTag))) & p.kDirBitsMask
			//shift := 12 - 4 + k*int(p.kDirBitsPerTag) - 64
			//if shift < 0 {
			//	tags[k] |= uint32(u2) << -shift
			//} else {
			//	tags[k] |= uint32(u2) >> shift
			//}
			//tags[k] &= p.kDirBitsMask
		}
	}

	/* codeword is the lowest 12 bits in the bucket */
	p.perm.Decode(codeword, &lowBits)

	tags[0] |= uint32(lowBits[0])
	tags[1] |= uint32(lowBits[1])
	tags[2] |= uint32(lowBits[2])
	tags[3] |= uint32(lowBits[3])
}

/* Tag = 4 low bits + x high bits
 * L L L L H H H H ...
 */
func (p *PackedTable) WriteBucket(i uint, tags [4]uint32) {
	p.sortTags(&tags)

	/* put in direct bits for each tag*/

	var lowBits [4]uint8
	var highBits [4]uint32

	lowBits[0] = uint8(tags[0] & 0x0f)
	lowBits[1] = uint8(tags[1] & 0x0f)
	lowBits[2] = uint8(tags[2] & 0x0f)
	lowBits[3] = uint8(tags[3] & 0x0f)

	highBits[0] = tags[0] & 0xfffffff0
	highBits[1] = tags[1] & 0xfffffff0
	highBits[2] = tags[2] & 0xfffffff0
	highBits[3] = tags[3] & 0xfffffff0

	// note that :  tags[j] = lowBits[j] | highBits[j]

	var codeword = p.perm.Encode(lowBits)
	pos := i * p.kBitsPerBucket >> 3
	switch p.kBitsPerBucket {
	//case 16:
	//	// 1 dirBits per tag
	//	var t = codeword | uint16(highBits[0]<<8) | uint16(highBits[1]<<9) |
	//		uint16(highBits[2]<<10) | uint16(highBits[3]<<11)
	//	b := make([]byte, 2)
	//	binary.LittleEndian.PutUint16(b, t)
	//	p.buckets[pos] = b[0]
	//	p.buckets[pos+1] = b[1]
	//case 20:
	//	// 2 dirBits per tag
	//	var t uint32
	//	t = binary.LittleEndian.Uint32([]byte{p.buckets[pos], p.buckets[pos+1], p.buckets[pos+2], p.buckets[pos+3]})
	//	if (i & 0x0001) == 0 {
	//		t &= 0xfff00000
	//		t |= uint32(codeword) | (highBits[0] << 8) |
	//			(highBits[1] << 10) | (highBits[2] << 12) |
	//			(highBits[3] << 14)
	//	} else {
	//		t &= 0xff00000f
	//		t |= uint32(codeword)<<4 | (highBits[0] << 12) |
	//			(highBits[1] << 14) | (highBits[2] << 16) |
	//			(highBits[3] << 18)
	//	}
	//	b := make([]byte, 4)
	//	binary.LittleEndian.PutUint32(b, t)
	//	p.buckets[pos] = b[0]
	//	p.buckets[pos+1] = b[1]
	//	p.buckets[pos+2] = b[2]
	//	p.buckets[pos+3] = b[3]
	//case 24:
	//	// 3 dirBits per tag
	//	var t uint32
	//	t = binary.LittleEndian.Uint32([]byte{p.buckets[pos], p.buckets[pos+1], p.buckets[pos+2], p.buckets[pos+3]})
	//	t &= 0xff000000
	//	t |= uint32(codeword) | (highBits[0] << 8) | (highBits[1] << 11) |
	//		(highBits[2] << 14) | (highBits[3] << 17)
	//	b := make([]byte, 4)
	//	binary.LittleEndian.PutUint32(b, t)
	//	p.buckets[pos] = b[0]
	//	p.buckets[pos+1] = b[1]
	//	p.buckets[pos+2] = b[2]
	//	p.buckets[pos+3] = b[3]
	//case 28:
	//	// 4 dirBits per tag
	//	var t uint32
	//	t = binary.LittleEndian.Uint32([]byte{p.buckets[pos], p.buckets[pos+1], p.buckets[pos+2], p.buckets[pos+3]})
	//	if (i & 0x0001) == 0 {
	//		t &= 0xf0000000
	//		t |= uint32(codeword) | (highBits[0] << 8) |
	//			(highBits[1] << 12) | (highBits[2] << 16) |
	//			(highBits[3] << 20)
	//	} else {
	//		t &= 0x0000000f
	//		t |= uint32(codeword)<<4 | (highBits[0] << 12) |
	//			(highBits[1] << 16) | (highBits[2] << 20) |
	//			(highBits[3] << 24)
	//	}
	//	b := make([]byte, 4)
	//	binary.LittleEndian.PutUint32(b, t)
	//	p.buckets[pos] = b[0]
	//	p.buckets[pos+1] = b[1]
	//	p.buckets[pos+2] = b[2]
	//	p.buckets[pos+3] = b[3]
	//case 32:
	//	// 5 dirBits per tag
	//	var t = uint32(codeword) | (highBits[0] << 8) | (highBits[1] << 13) |
	//		(highBits[2] << 18) | (highBits[3] << 23)
	//	b := make([]byte, 4)
	//	binary.LittleEndian.PutUint32(b, t)
	//	p.buckets[pos] = b[0]
	//	p.buckets[pos+1] = b[1]
	//	p.buckets[pos+2] = b[2]
	//	p.buckets[pos+3] = b[3]
	//case 48:
	//	// 9 dirBits per tag
	//	var t uint64
	//	t = binary.LittleEndian.Uint64([]byte{p.buckets[pos], p.buckets[pos+1], p.buckets[pos+2], p.buckets[pos+3], p.buckets[pos+4], p.buckets[pos+5], p.buckets[pos+6], p.buckets[pos+7]})
	//	t &= 0xffff000000000000
	//	t |= uint64(codeword) | uint64(highBits[0])<<8 |
	//		uint64(highBits[1])<<17 | uint64(highBits[2])<<26 |
	//		uint64(highBits[3])<<35
	//	b := make([]byte, 8)
	//	binary.LittleEndian.PutUint64(b, t)
	//	p.buckets[pos] = b[0]
	//	p.buckets[pos+1] = b[1]
	//	p.buckets[pos+2] = b[2]
	//	p.buckets[pos+3] = b[3]
	//	p.buckets[pos+4] = b[4]
	//	p.buckets[pos+5] = b[5]
	//	p.buckets[pos+6] = b[6]
	//	p.buckets[pos+7] = b[7]
	//case 64:
	//	// 13 dirBits per tag
	//	var t = uint64(codeword) | uint64(highBits[0])<<8 |
	//		uint64(highBits[1])<<21 | uint64(highBits[2])<<34 |
	//		uint64(highBits[3])<<47
	//	b := make([]byte, 8)
	//	binary.LittleEndian.PutUint64(b, t)
	//	p.buckets[pos] = b[0]
	//	p.buckets[pos+1] = b[1]
	//	p.buckets[pos+2] = b[2]
	//	p.buckets[pos+3] = b[3]
	//	p.buckets[pos+4] = b[4]
	//	p.buckets[pos+5] = b[5]
	//	p.buckets[pos+6] = b[6]
	//	p.buckets[pos+7] = b[7]
	default:
		// tag is max 32bit, store 31bit per tag, so max occupies 16 bytes
		b1 := make([]byte, 8)
		b2 := make([]byte, 8)

		var u1, u2 uint64
		u1 = uint64(codeword)
		for k := 0; k < 4; k++ {
			u1 |= uint64(highBits[k]) << (12 - 4 + k*int(p.kDirBitsPerTag))
			shift := 12 - 4 + k*int(p.kDirBitsPerTag) - 64
			if shift < 0 {
				u2 |= uint64(highBits[k]) >> -shift
			} else {
				u2 |= uint64(highBits[k]) << shift
			}
		}
		binary.LittleEndian.PutUint64(b1, u1)
		binary.LittleEndian.PutUint64(b2, u2)

		for k := uint(0); k < 8; k++ {
			if k+1 <= p.kBytesPerBucket {
				p.buckets[pos+k] = b1[k]
			} else {
				break
			}
		}
		if p.kBytesPerBucket > 8 {
			for k := uint(0); k < 8; k++ {
				if k+1 <= p.kBytesPerBucket-8 {
					p.buckets[pos+8+k] = b2[k]
				} else {
					break
				}
			}
		}
	}

}

func (p *PackedTable) FindTagInBuckets(i1, i2 uint, tag uint32) bool {
	var tags1, tags2 [4]uint32
	p.ReadBucket(i1, &tags1)
	p.ReadBucket(i2, &tags2)

	return (tags1[0] == tag) || (tags1[1] == tag) || (tags1[2] == tag) ||
		(tags1[3] == tag) || (tags2[0] == tag) || (tags2[1] == tag) ||
		(tags2[2] == tag) || (tags2[3] == tag)
}
func (p *PackedTable) FindTagInBucket(i uint, tag uint32) bool {
	var tags [4]uint32
	p.ReadBucket(i, &tags)

	return (tags[0] == tag) || (tags[1] == tag) || (tags[2] == tag) || (tags[3] == tag)
}

func (p *PackedTable) DeleteTagFromBucket(i uint, tag uint32) bool {
	var tags [4]uint32
	p.ReadBucket(i, &tags)
	for j := 0; j < 4; j++ {
		if tags[j] == tag {
			tags[j] = 0
			p.WriteBucket(i, tags)
			return true
		}
	}
	return false
}

func (p *PackedTable) InsertTagToBucket(i uint, tag uint32, kickOut bool, oldTag *uint32) bool {
	var tags [4]uint32
	p.ReadBucket(i, &tags)
	for j := 0; j < 4; j++ {
		if tags[j] == 0 {
			tags[j] = tag
			p.WriteBucket(i, tags)
			//p.PrintBucket(i)

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

func (p *PackedTable) Info() string {
	return fmt.Sprintf("PackedHashtable with tag size: %v bits \n"+
		"\t\t4 packed bits(3 bits after compression) and %v direct bits\n"+
		"\t\tAssociativity: 4 \n"+
		"\t\tTotal # of rows: %v\n"+
		"\t\tTotal # slots: %v\n",
		p.bitsPerTag, p.kDirBitsPerTag, p.numBuckets, p.SizeInTags())
}
