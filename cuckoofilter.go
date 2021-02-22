/*
 * Copyright (C) linvon
 * Date  2021/2/18 10:29
 */

package cuckoo

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/dgryski/go-metro"
)

// maximum number of cuckoo kicks before claiming failure
const kMaxCuckooCount uint = 500

const (
	TableTypeSingle = 0
	TableTypePacked = 1
)

type Table interface {
	Init(tagsPerBucket, bitsPerTag, num uint)
	NumBuckets() uint
	FindTagInBuckets(i1, i2 uint, tag uint32) bool
	DeleteTagFromBucket(i uint, tag uint32) bool
	InsertTagToBucket(i uint, tag uint32, kickOut bool, oldTag *uint32) bool
	SizeInTags() uint
	SizeInBytes() uint
	Info() string
	BitsPerItem() uint
	Encode() []byte
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

type VictimCache struct {
	index uint
	tag   uint32
	used  bool
}

type Filter struct {
	victim      VictimCache
	numItems    uint
	table       Table
	bitsPerItem uint
}

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
	table := getTable(tableType).(Table)
	table.Init(tagsPerBucket, bitsPerItem, numBuckets)
	return &Filter{
		table:       table,
		bitsPerItem: table.BitsPerItem(),
	}
}

func (f *Filter) indexHash(hv uint32) uint {
	// table.NumBuckets is always a power of two, so modulo can be replaced with bitwise-and:
	return uint(hv) & (f.table.NumBuckets() - 1)
}

func (f *Filter) tagHash(hv uint32) uint32 {
	var tag uint32
	tag = hv%((1<<f.bitsPerItem)-1) + 1
	return tag
}
func (f *Filter) generateIndexTagHash(item []byte, index *uint, tag *uint32) {
	hash := metro.Hash64(item, 1337)
	*index = f.indexHash(uint32(hash >> 32))
	*tag = f.tagHash(uint32(hash))
}
func (f *Filter) altIndex(index uint, tag uint32) uint {
	// 0x5bd1e995 is the hash constant from MurmurHash2
	return f.indexHash(uint32(index) ^ (tag * 0x5bd1e995))
}

func (f *Filter) Size() uint {
	return f.numItems
}
func (f *Filter) LoadFactor() float64 {
	return 1.0 * float64(f.Size()) / float64(f.table.SizeInTags())
}
func (f *Filter) SizeInBytes() uint {
	return f.table.SizeInBytes()
}

func (f *Filter) BitsPerItem() float64 {
	return 8.0 * float64(f.table.SizeInBytes()) / float64(f.Size())
}

func (f *Filter) Add(item []byte) bool {
	var i uint
	var tag uint32

	if f.victim.used {
		return false
	}
	f.generateIndexTagHash(item, &i, &tag)
	return f.addImpl(i, tag)
}

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
	for count = 0; count < kMaxCuckooCount; count++ {
		kickOut := count > 0
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

func (f *Filter) Contain(key []byte) bool {
	var found bool
	var i1, i2 uint
	var tag uint32

	f.generateIndexTagHash(key, &i1, &tag)
	i2 = f.altIndex(i1, tag)

	if i1 != f.altIndex(i2, tag) {
		panic("hash err")
	}
	found = f.victim.used && tag == f.victim.tag && (i1 == f.victim.index || i2 == f.victim.index)

	if found || f.table.FindTagInBuckets(i1, i2, tag) {
		return true
	} else {
		return false
	}
}

func (f *Filter) Delete(key []byte) bool {
	var i1, i2 uint
	var tag uint32

	f.generateIndexTagHash(key, &i1, &tag)
	i2 = f.altIndex(i1, tag)

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
	return float64(fp) / float64(rounds)
}

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
func (f *Filter) Encode() []byte {
	b := make([]byte, bytesPerUint32)
	binary.LittleEndian.PutUint32(b, uint32(f.numItems))

	return append(b, f.table.Encode()...)
}

// Decode returns a Cuckoo Filter from a byte slice
func Decode(bytes []byte) (*Filter, error) {
	if len(bytes) < 11 {
		return nil, errors.New("unexpected bytes length")
	}
	numItems := uint(binary.LittleEndian.Uint32(bytes[0:4]))
	tableType := uint(bytes[4])
	table := getTable(tableType).(Table)
	err := table.Decode(bytes[4:])
	if err != nil {
		return nil, err
	}
	return &Filter{
		table:       table,
		numItems:    numItems,
		bitsPerItem: table.BitsPerItem(),
	}, nil
}
