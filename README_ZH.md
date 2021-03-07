# cuckoo-filter
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)  

cuckoo-filter 的 go 实现版本. 可按你的配置来定制过滤器参数

移植于 [efficient/cuckoofilter](https://github.com/efficient/cuckoofilter)

[English Version](./README.md)

概述
--------
布谷鸟过滤器是一种在近似集合隶属查询时替代布隆过滤器的数据结构。布隆过滤器是众所周知的一种用于查询类似于“x是否在集合中？”这类问题，且非常节省空间的数据结构，但不支持删除。其支持删除的相关变种（如计数布隆过滤器）通常需要更多的空间。

布谷鸟过滤器可以灵活地动态添加和删除项。布谷鸟过滤器是基于布谷鸟哈希的（这也是为什么称为布谷鸟过滤器）。 它本质上是一个存储每个键的指纹的布谷鸟哈希表。布谷鸟哈希表可以非常紧凑，因此对于需要更低假阳性率（<3%）的应用程序，布谷鸟过滤器可以比传统的布隆过滤器节省更多空间。

有关算法和引用的详细信息，请参阅：

["Cuckoo Filter: Practically Better Than Bloom"](http://www.cs.cmu.edu/~binfan/papers/conext14_cuckoofilter.pdf) in proceedings of ACM CoNEXT 2014 by Bin Fan, Dave Andersen and Michael Kaminsky

[中文翻译版论文](http://www.linvon.cn/posts/cuckoo/)


## 实现细节

本库的具体实现细节以及使用方法可以参考  [布谷鸟过滤器实战指南](http://www.linvon.cn/posts/%E5%B8%83%E8%B0%B7%E9%B8%9F%E8%BF%87%E6%BB%A4%E5%99%A8%E5%AE%9E%E6%88%98%E6%8C%87%E5%8D%97/)

上述的论文提供了几个参数供选择 

1. 桶大小(b)：一个桶存储几个指纹
2. 指纹大小(f)：每个指纹存储的键的哈希值的位数

在其他的实现中:

- [seiflotfy/cuckoofilter](https://github.com/seiflotfy/cuckoofilter) 使用 b=4, f=8 bit，其假阳性率趋近于 `r ~= 0.03`。
- [panmari/cuckoofilter](https://github.com/panmari/cuckoofilter) 使用 b=4, f=16 bit，其假阳性率趋近于 `r ~= 0.0001`。
- [irfansharif/cfilter](https://github.com/irfansharif/cfilter) 可以调整 b 和 f，但只能调整 f 为 8 的倍数，即以字节为单位。

在这个实现中, 你可以调整 b 和 f 为任意你想要的值，并且论文中提到的半排序桶也是可以使用的, 该方法可以对每一项节省一个 bit。

##### 为什么定制很重要?

根据论文

- 不同的桶大小会产生不同的过滤器负载因子，这代表着过滤器的最大空间利用率 
- 不同的桶大小适用于不同的目标假阳性率
- 为了保持假阳性率不变，桶大小越大，需要的指纹大小就越大

假定我们需要的假阳性率为 `r` 

> 当r>0.002时。每桶有两个条目比每桶使用四个条目产生的结果略好；当ϵ减小到0.00001<r≤0.002时，每个桶四个条目可以最小化空间。

选定了 `b`, 他们建议按如下公式选择 `f`

    f >= log2(2b/r) bits

同时，注意当使用桶大小为b = 2, 4 or 8时，对应的负载因子为 84%, 95% or 98%。

##### 想了解更多关于参数选择的内容，请参考论文的第五章节

注意: 通常情况下 b = 8 就足够了，由于没有更多数据的支持，我们建议你从2、4、8中选择桶大小。而 f 最大为 32 bits。

## 参考用例:

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

