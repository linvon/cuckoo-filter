# cuckoo-filter
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)  

cuckoo-filter go implement. Config by you

transplant from [efficient/cuckoofilter](https://github.com/efficient/cuckoofilter)

[中文文档](./README_ZH.md)

Overview
--------
Cuckoo filter is a Bloom filter replacement for approximated set-membership queries. While Bloom filters are well-known space-efficient data structures to serve queries like "if item x is in a set?", they do not support deletion. Their variances to enable deletion (like counting Bloom filters) usually require much more space.

Cuckoo ﬁlters provide the ﬂexibility to add and remove items dynamically. A cuckoo filter is based on cuckoo hashing (and therefore named as cuckoo filter).  It is essentially a cuckoo hash table storing each key's fingerprint. Cuckoo hash tables can be highly compact, thus a cuckoo filter could use less space than conventional Bloom ﬁlters, for applications that require low false positive rates (< 3%).

For details about the algorithm and citations please use:

["Cuckoo Filter: Practically Better Than Bloom"](http://www.cs.cmu.edu/~binfan/papers/conext14_cuckoofilter.pdf) in proceedings of ACM CoNEXT 2014 by Bin Fan, Dave Andersen and Michael Kaminsky

## Implementation details

The paper cited above leaves several parameters to choose. 

2. Bucket size(b): Number of fingerprints per bucket
3. Fingerprints size(f): Fingerprints bits size of hashtag

In other implementation:

- [seiflotfy/cuckoofilter](https://github.com/seiflotfy/cuckoofilter) use b=4, f=8 bit, which correspond to a false positive rate of `r ~= 0.03`.
- [panmari/cuckoofilter](https://github.com/panmari/cuckoofilter) use b=4, f=16 bit, which correspond to a false positive rate of `r ~= 0.0001`.
- [irfansharif/cfilter](https://github.com/irfansharif/cfilter) can adjust b and f, but only can adjust f to 8x, which means it is in Bytes.

In this implementation, you can adjust b and f to any value you want in `TableTypeSingle` type implementation.

In addition, the Semi-sorting Buckets mentioned in paper which can save 1 bit per item is also available in `TableTypePacked` type,
note that b=4, only f is adjustable.

##### Why custom is important?

According to paper

- Different  bucket size result in different filter loadfactor, which means occupancy rate of filter 
- Different bucket size is suitable for different target false positive rate
- To keep a false positive rate, bigger bucket size, bigger fingerprint size

 Given a target false positive rate of `r` 

> when  r > 0.002, having two entries per bucket yields slightly better results than using four entries per bucket; when decreases to 0.00001 < r ≤ 0.002, four entries per bucket minimizes space.

with a bucket size `b`, they suggest choosing the fingerprint size `f` using

    f >= log2(2b/r) bits

as the same time, notice that we got loadfactor 84%, 95% or 98% when using bucket size b = 2, 4 or 8

##### To know more about parameter choosing, refer to paper's section 5

Note: generally b = 8 is enough, without more data support, we suggest you choosing b from 2, 4 or 8. And f is max 32 bits

## Example usage:

``` go
package main

import (
	"fmt"
	"github.com/linvon/cuckoo-filter"
)

func main() {
	cf := cuckoo.NewFilter(4, 9, 3900, cuckoo.TableTypePacked)
	fmt.Println(cf.Info())
	fmt.Println(cf.FalsePositiveRate())

	a := []byte("A")
	cf.Add(a)
	fmt.Println(cf.Contain(a))
	fmt.Println(cf.Size())

	b := cf.Encode()
	ncf, _ := cuckoo.Decode(b)
	fmt.Println(ncf.Contain(a))

	cf.Delete(a)
	fmt.Println(cf.Size())
}
```

