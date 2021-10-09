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
const kMaxCuckooCount uint = 500

const (
	// TableTypeSingle normal single table
	TableTypeSingle = 0
	// TableTypePacked packed table, use semi-sort to save 1 bit per item
	TableTypePacked = 1
)

type table interface {
	Init(tagsPerBucket, bitsPerTag, num uint, initialBucketsHint []byte) error
	NumBuckets() uint
	FindTagInBuckets(i1, i2 uint, tag uint32) bool
	DeleteTagFromBucket(i uint, tag uint32) bool
	InsertTagToBucket(i uint, tag uint32, kickOut bool, oldTag *uint32) bool
	SizeInTags() uint
	SizeInBytes() uint
	Info() string
	BitsPerItem() uint
	Reader() (io.Reader, uint)
	Decode([]byte) error
	Reset()
}

func getTable(tableType uint) interface{} {
	switch tableType {
	case TableTypePacked:
		return NewPackedTable()
	default:
		return NewSingleTable()
	}
}

type victimCache struct {
	index uint
	tag   uint32
	used  bool
}

const filterMetadataSize = 3*bytesPerUint32 + 1

// Filter cuckoo filter type struct
type Filter struct {
	victim   victimCache
	numItems uint
	table    table
}

//NewFilter return a new initialized filter
/*
	tagsPerBucket: num of tags for each bucket, which is b in paper. tag is fingerprint, which is f in paper.
	bitPerItem: num of bits for each item, which is length of tag(fingerprint)
	maxNumKeys: num of keys that filter will store. this value should close to and lower
				nextPow2(maxNumKeys/tagsPerBucket) * maxLoadFactor. cause table.NumBuckets is always a power of two
*/
func NewFilter(tagsPerBucket, bitsPerItem, maxNumKeys, tableType uint) *Filter {
	numBuckets := getNextPow2(uint64(maxNumKeys / tagsPerBucket))
	if float64(maxNumKeys)/float64(numBuckets*tagsPerBucket) > maxLoadFactor(tagsPerBucket) {
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

func (f *Filter) indexHash(hv uint32) uint {
	// table.NumBuckets is always a power of two, so modulo can be replaced with bitwise-and:
	return uint(hv) & (f.table.NumBuckets() - 1)
}

func (f *Filter) tagHash(hv uint32) uint32 {
	return hv%((1<<f.table.BitsPerItem())-1) + 1
}

func (f *Filter) generateIndexTagHash(item []byte) (index uint, tag uint32) {
	hash := metro.Hash64(item, 1337)
	index = f.indexHash(uint32(hash >> 32))
	tag = f.tagHash(uint32(hash))
	return
}

func (f *Filter) altIndex(index uint, tag uint32) uint {
	// 0x5bd1e995 is the hash constant from MurmurHash2
	return f.indexHash(uint32(index) ^ (tag * 0x5bd1e995))
}

// Size return num of items that filter store
func (f *Filter) Size() uint {
	var c uint
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
func (f *Filter) SizeInBytes() uint {
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

func (f *Filter) addImpl(i uint, tag uint32) bool {
	curIndex := i
	curTag := tag
	var oldTag uint32

	var count uint
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
func (f *Filter) Encode() ([]byte, error) {
	filterReader, filterSize := f.EncodeReader()
	buf := make([]byte, filterSize)
	if _, err := io.ReadFull(filterReader, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// EncodeReader returns a reader representing a Cuckoo filter
func (f *Filter) EncodeReader() (io.Reader, uint) {
	var metadata [filterMetadataSize]byte

	for i, n := range []uint32{uint32(f.numItems), uint32(f.victim.index), f.victim.tag} {
		binary.LittleEndian.PutUint32(metadata[i*bytesPerUint32:], n)
	}

	victimUsed := byte(0)
	if f.victim.used {
		victimUsed = byte(1)
	}
	metadata[bytesPerUint32*3] = victimUsed
	tableReader, tableEncodedSize := f.table.Reader()
	return io.MultiReader(bytes.NewReader(metadata[:]), tableReader), uint(len(metadata)) + tableEncodedSize
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
	numItems := uint(binary.LittleEndian.Uint32(b[0*bytesPerUint32:]))
	curIndex := uint(binary.LittleEndian.Uint32(b[1*bytesPerUint32:]))
	curTag := binary.LittleEndian.Uint32(b[2*1*bytesPerUint32:])
	used := b[12] == byte(1)
	tableType := uint(b[13])
	table := getTable(tableType).(table)
	if err := table.Decode(b[13:]); err != nil {
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
