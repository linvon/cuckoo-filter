/*
 * Copyright (C) linvon
 * Date  2021/2/18 10:29
 */

package cuckoo

import (
	"fmt"
	"github.com/dgryski/go-metro"
)

// status returned by a cuckoo filter operation
const (
	Ok             = 0
	NotFound       = 1
	NotEnoughSpace = 2
	Existed        = 3
)

// maximum number of cuckoo kicks before claiming failure
const kMaxCuckooCount uint = 500

type Table interface {
	Init(tagsPerBucket, bitsPerTag, num uint)
	NumBuckets() uint
	FindTagInBuckets(i1, i2 uint, tag uint32) bool
	DeleteTagFromBucket(i uint, tag uint32) bool
	InsertTagToBucket(i uint, tag uint32, kickOut bool, oldTag *uint32) bool
	SizeInTags() uint
	SizeInBytes() uint
	Info() string
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

func NewFilter(maxNumKeys, tagsPerBucket, bitPerItem uint, table Table) *Filter {
	numBuckets := getNextPow2(uint64(maxNumKeys / tagsPerBucket))
	if float64(maxNumKeys)/float64(numBuckets*tagsPerBucket) > maxLoadFactor(tagsPerBucket) {
		numBuckets <<= 1
	}
	if numBuckets == 0 {
		numBuckets = 1
	}
	table.Init(tagsPerBucket, bitPerItem, numBuckets)
	return &Filter{
		table:       table,
		bitsPerItem: bitPerItem,
	}
}

func (f *Filter) IndexHash(hv uint32) uint {
	// table_->num_buckets is always a power of two, so modulo can be replaced with bitwise-and:
	return uint(hv) & (f.table.NumBuckets() - 1)
}

func (f *Filter) TagHash(hv uint32) uint32 {
	var tag uint32
	tag = hv%((1<<f.bitsPerItem)-1) + 1
	return tag
}
func (f *Filter) GenerateIndexTagHash(item []byte, index *uint, tag *uint32) {
	hash := metro.Hash64(item, 1337)
	*index = f.IndexHash(uint32(hash >> 32))
	*tag = f.TagHash(uint32(hash))
}
func (f *Filter) AltIndex(index uint, tag uint32) uint {
	// 0x5bd1e995 is the hash constant from MurmurHash2
	return f.IndexHash(uint32(index) ^ (tag * 0x5bd1e995))
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

func (f *Filter) Add(item []byte) uint {
	var i uint
	var tag uint32

	if f.victim.used {
		return NotEnoughSpace
	}
	f.GenerateIndexTagHash(item, &i, &tag)
	return f.AddImpl(i, tag)
}

func (f *Filter) AddUnique(item []byte) uint {
	if f.Contain(item) == Ok {
		return Existed
	}
	return f.Add(item)
}

func (f *Filter) AddImpl(i uint, tag uint32) uint {
	curIndex := i
	curTag := tag
	var oldTag uint32

	var count uint
	for count = 0; count < kMaxCuckooCount; count++ {
		kickout := count > 0
		oldTag = 0
		if f.table.InsertTagToBucket(curIndex, curTag, kickout, &oldTag) {
			f.numItems++
			return Ok
		}
		if kickout {
			curTag = oldTag
		}
		curIndex = f.AltIndex(curIndex, curTag)
	}

	f.victim.index = curIndex
	f.victim.tag = curTag
	f.victim.used = true
	return Ok
}

func (f *Filter) Contain(key []byte) uint {
	var found bool
	var i1, i2 uint
	var tag uint32

	f.GenerateIndexTagHash(key, &i1, &tag)
	i2 = f.AltIndex(i1, tag)

	if i1 != f.AltIndex(i2, tag) {
		panic("hash err")
	}
	found = f.victim.used && tag == f.victim.tag && (i1 == f.victim.index || i2 == f.victim.index)

	if found || f.table.FindTagInBuckets(i1, i2, tag) {
		return Ok
	} else {
		return NotFound
	}
}

func (f *Filter) Delete(key []byte) uint {
	var i1, i2 uint
	var tag uint32

	f.GenerateIndexTagHash(key, &i1, &tag)
	i2 = f.AltIndex(i1, tag)

	if f.table.DeleteTagFromBucket(i1, tag) || f.table.DeleteTagFromBucket(i2, tag) {
		f.numItems--
		goto TryEliminateVictim
	} else if f.victim.used && tag == f.victim.tag && (i1 == f.victim.index || i2 == f.victim.index) {
		f.victim.used = false
		return Ok
	} else {
		return NotFound
	}

TryEliminateVictim:
	if f.victim.used {
		f.victim.used = false
		i := f.victim.index
		tag = f.victim.tag
		f.AddImpl(i, tag)
	}
	return Ok
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
