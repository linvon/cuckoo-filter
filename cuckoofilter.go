/*
 * Copyright (C) linvon
 * Date  2021/2/18 10:29
 */

package cuckoo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/dgryski/go-metro"
)

// maximum number of cuckoo kicks before claiming failure
const kMaxCuckooCount uint64 = 500

type TableType uint32

const (
	// TableTypeSingle normal single table
	TableTypeSingle TableType = 0
	// TableTypePacked packed table, use semi-sort to save 1 bit per item
	TableTypePacked TableType = 1
)

type table interface {
	Init(tagsPerBucket, bitsPerTag uint, num uint64, initialBucketsHint []byte) error
	NumBuckets() uint64
	FindTagInBuckets(i1, i2 uint64, tag uint32) bool
	DeleteTagFromBucket(i uint64, tag uint32) bool
	InsertTagToBucket(i uint64, tag uint32, kickOut bool, oldTag *uint32) bool
	SizeInTags() uint64
	SizeInBytes() uint64
	Info() string
	BitsPerItem() uint
	Reader(legacy bool) (io.Reader, uint64)
	Decode(legacy bool, _ []byte) error
	Reset()
}

func getTable(tableType TableType) interface{} {
	switch tableType {
	case TableTypePacked:
		return NewPackedTable()
	default:
		return NewSingleTable()
	}
}

type victimCache struct {
	index uint64
	tag   uint32
	used  bool
}

// Filter cuckoo filter type struct
type Filter struct {
	victim   victimCache
	numItems uint64
	table    table
}

// NewFilter return a new initialized filter
/*
	tagsPerBucket: num of tags for each bucket, which is b in paper. tag is fingerprint, which is f in paper.
	bitPerItem: num of bits for each item, which is length of tag(fingerprint) (max is 32)
	maxNumKeys: num of keys that filter will store. this value should close to and lower
				nextPow2(maxNumKeys/tagsPerBucket) * maxLoadFactor. cause table.NumBuckets is always a power of two
*/
func NewFilter(tagsPerBucket, bitsPerItem uint, maxNumKeys uint64, tableType TableType) *Filter {
	numBuckets := getNextPow2(maxNumKeys / uint64(tagsPerBucket))
	if float64(maxNumKeys)/float64(numBuckets*uint64(tagsPerBucket)) > maxLoadFactor(tagsPerBucket) {
		numBuckets <<= 1
	}
	if numBuckets == 0 {
		numBuckets = 1
	}
	table := getTable(tableType).(table)
	_ = table.Init(tagsPerBucket, bitsPerItem, numBuckets, nil)
	return &Filter{
		table: table,
	}
}

func (f *Filter) indexHash(hv uint64) uint64 {
	// table.NumBuckets is always a power of two, so modulo can be replaced with bitwise-and:
	return hv & (f.table.NumBuckets() - 1)
}

func (f *Filter) tagHash(hv uint64) uint32 {
	return uint32(hv)%((1<<f.table.BitsPerItem())-1) + 1
}

func (f *Filter) generateIndexTagHash(item []byte) (index uint64, tag uint32) {
	hash1, hash2 := metro.Hash128(item, 1337)
	index = f.indexHash(hash1)
	tag = f.tagHash(hash2)
	return
}

func (f *Filter) altIndex(index uint64, tag uint32) uint64 {
	// 0xc6a4a7935bd1e995 is the hash constant from MurmurHash64A
	return f.indexHash(index ^ (uint64(tag) * 0xc6a4a7935bd1e995))
}

// Size return num of items that filter store
func (f *Filter) Size() uint64 {
	var c uint64
	if f.victim.used {
		c = 1
	}
	return f.numItems + c
}

// LoadFactor return current filter's loadFactor
func (f *Filter) LoadFactor() float64 {
	return 1.0 * float64(f.Size()) / float64(f.table.SizeInTags())
}

// SizeInBytes return bytes occupancy of filter's table
func (f *Filter) SizeInBytes() uint64 {
	return f.table.SizeInBytes()
}

// BitsPerItem return bits occupancy per item of filter's table
func (f *Filter) BitsPerItem() float64 {
	return 8.0 * float64(f.table.SizeInBytes()) / float64(f.Size())
}

// Add add an item into filter, return false when filter is full
func (f *Filter) Add(item []byte) bool {
	if f.victim.used {
		return false
	}
	i, tag := f.generateIndexTagHash(item)
	return f.addImpl(i, tag)
}

// AddUnique add an item into filter, return false when filter already contains it or filter is full
func (f *Filter) AddUnique(item []byte) bool {
	if f.Contain(item) {
		return false
	}
	return f.Add(item)
}

func (f *Filter) addImpl(i uint64, tag uint32) bool {
	curIndex := i
	curTag := tag
	var oldTag uint32

	var count uint64
	var kickOut bool
	for count = 0; count < kMaxCuckooCount; count++ {
		kickOut = count > 0
		oldTag = 0
		if f.table.InsertTagToBucket(curIndex, curTag, kickOut, &oldTag) {
			f.numItems++
			return true
		}
		if kickOut {
			curTag = oldTag
		}
		curIndex = f.altIndex(curIndex, curTag)
	}

	f.victim.index = curIndex
	f.victim.tag = curTag
	f.victim.used = true
	return true
}

// Contain return if filter contains an item
func (f *Filter) Contain(key []byte) bool {
	i1, tag := f.generateIndexTagHash(key)
	i2 := f.altIndex(i1, tag)

	hit := f.victim.used && tag == f.victim.tag && (i1 == f.victim.index || i2 == f.victim.index)

	if hit || f.table.FindTagInBuckets(i1, i2, tag) {
		return true
	}
	return false
}

// Delete delete item from filter, return false when item not exist
func (f *Filter) Delete(key []byte) bool {
	i1, tag := f.generateIndexTagHash(key)
	i2 := f.altIndex(i1, tag)

	if f.table.DeleteTagFromBucket(i1, tag) || f.table.DeleteTagFromBucket(i2, tag) {
		f.numItems--
		goto TryEliminateVictim
	} else if f.victim.used && tag == f.victim.tag && (i1 == f.victim.index || i2 == f.victim.index) {
		f.victim.used = false
		return true
	} else {
		return false
	}

TryEliminateVictim:
	if f.victim.used {
		f.victim.used = false
		i := f.victim.index
		tag = f.victim.tag
		f.addImpl(i, tag)
	}
	return true
}

// Reset reset the filter
func (f *Filter) Reset() {
	f.table.Reset()
	f.numItems = 0
	f.victim.index = 0
	f.victim.tag = 0
	f.victim.used = false
}

// FalsePositiveRate return the False Positive Rate of filter
// Notice that this will reset filter
func (f *Filter) FalsePositiveRate() float64 {
	n1 := make([]byte, 4)
	f.Reset()
	n := f.table.SizeInTags()
	for i := uint32(0); i < uint32(n); i++ {
		binary.BigEndian.PutUint32(n1, i)
		f.Add(n1)
	}
	var rounds uint32 = 100000
	fp := 0
	for i := uint32(0); i < rounds; i++ {
		binary.BigEndian.PutUint32(n1, i+uint32(n)+1)
		if f.Contain(n1) {
			fp++
		}
	}
	f.Reset()
	return float64(fp) / float64(rounds)
}

// Info return filter's detail info
func (f *Filter) Info() string {
	return fmt.Sprintf("CuckooFilter Status:\n"+
		"\t\t%v\n"+
		"\t\tKeys stored: %v\n"+
		"\t\tLoad factor: %v\n"+
		"\t\tHashtable size: %v KB\n"+
		"\t\tbit/key:   %v\n",
		f.table.Info(), f.Size(), f.LoadFactor(), f.table.SizeInBytes()>>10, f.BitsPerItem())
}

// Encode returns a byte slice representing a Cuckoo filter
func (f *Filter) Encode(legacy bool) ([]byte, error) {
	filterReader, filterSize := f.EncodeReader(legacy)
	buf := make([]byte, filterSize)
	if _, err := io.ReadFull(filterReader, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

const (
	// uint32(numItems), uint32(victim.index), uint32(victim.tag), byte(victim.used)
	filterMetadataSizeLegacy = 3*bytesPerUint32 + 1

	//  uint64(numItems), uint64(victim.index), uint32(victim.tag), byte(victim.used)
	filterMetadataSize = 2*bytesPerUint64 + bytesPerUint32 + 1
)

// In the legacy serialization format, there are 3 uint32s and then a byte for "victimUsed" which is a boolean 0 or 1.
// We need a way to distinguish between the legacy format and the new format, so we can use that byte as a marker
// (0/1 means the legacy format, other value means the new format).
// In the new format the first 13 bytes are not used, just markers, the actual serialization starts after the marker.
// The marker is hex of "IMNOTLEGACY!!" :).
var newFormatMarker = [filterMetadataSizeLegacy]byte{0x49, 0x4D, 0x4E, 0x4F, 0x54, 0x4C, 0x45, 0x47, 0x41, 0x43, 0x59, 0x21, 0x21}

// EncodeReader returns a reader representing a Cuckoo filter
func (f *Filter) EncodeReader(legacy bool) (io.Reader, uint64) {
	if legacy {
		return f.encodeReaderLegacyMaxUint32()
	}

	var metadata [filterMetadataSize]byte

	for i, n := range []uint64{f.numItems, f.victim.index} {
		binary.LittleEndian.PutUint64(metadata[i*bytesPerUint64:], n)
	}

	binary.LittleEndian.PutUint32(metadata[2*bytesPerUint64:], f.victim.tag)

	victimUsed := byte(0)
	if f.victim.used {
		victimUsed = byte(1)
	}
	metadata[2*bytesPerUint64+bytesPerUint32] = victimUsed
	tableReader, tableEncodedSize := f.table.Reader(false)
	return io.MultiReader(bytes.NewReader(newFormatMarker[:]), bytes.NewReader(metadata[:]), tableReader), uint64(len(newFormatMarker)) + uint64(len(metadata)) + tableEncodedSize
}

// encodeReaderLegacyMaxUint32 returns a reader representing a Cuckoo filter encoded in the legacy mode that supports up to max(uint32) items.
func (f *Filter) encodeReaderLegacyMaxUint32() (io.Reader, uint64) {
	var metadata [filterMetadataSizeLegacy]byte

	for i, n := range []uint32{uint32(f.numItems), uint32(f.victim.index), f.victim.tag} {
		binary.LittleEndian.PutUint32(metadata[i*bytesPerUint32:], n)
	}

	victimUsed := byte(0)
	if f.victim.used {
		victimUsed = byte(1)
	}
	metadata[bytesPerUint32*3] = victimUsed
	tableReader, tableEncodedSize := f.table.Reader(true)
	return io.MultiReader(bytes.NewReader(metadata[:]), tableReader), uint64(len(metadata)) + tableEncodedSize
}

// Decode returns a Cuckoo Filter using a copy of the provided byte slice.
func Decode(b []byte) (*Filter, error) {
	copiedBytes := make([]byte, len(b))
	copy(copiedBytes, b)
	return DecodeFrom(copiedBytes)
}

// DecodeFrom returns a Cuckoo Filter using the exact provided byte slice (no copy).
func DecodeFrom(b []byte) (*Filter, error) {
	if len(b) < 20 {
		return nil, errors.New("unexpected bytes length")
	}

	curOffset := uint64(0)
	legacy := uint(b[len(newFormatMarker)-1]) <= 1

	// Skip the marker if it's the new format.
	if !legacy {
		curOffset += uint64(len(newFormatMarker))
	}

	var numItems uint64
	if legacy {
		numItems = uint64(binary.LittleEndian.Uint32(b[curOffset:]))
		curOffset += bytesPerUint32
	} else {
		numItems = binary.LittleEndian.Uint64(b[curOffset:])
		curOffset += bytesPerUint64
	}

	var curIndex uint64
	if legacy {
		curIndex = uint64(binary.LittleEndian.Uint32(b[curOffset:]))
		curOffset += bytesPerUint32
	} else {
		curIndex = binary.LittleEndian.Uint64(b[curOffset:])
		curOffset += bytesPerUint64
	}

	curTag := binary.LittleEndian.Uint32(b[curOffset:])
	curOffset += bytesPerUint32

	used := b[curOffset] == byte(1)

	tableOffset := curOffset + 1
	tableType := TableType(b[tableOffset])
	table := getTable(tableType).(table)
	if err := table.Decode(legacy, b[tableOffset:]); err != nil {
		return nil, err
	}
	return &Filter{
		table:    table,
		numItems: numItems,
		victim: victimCache{
			index: curIndex,
			tag:   curTag,
			used:  used,
		},
	}, nil
}
