package cmap

import (
	"math"
	"sync/atomic"
)

// ConcurrentMap 代表并发安全的字典接口
type ConcurrentMap interface {
	// Concurrency 返回并发量
	Concurrency() int
	// Put  推送一个键-元素对
	// 注意!参数element的值不能为nil
	// 第一个返回值表示是否新增了键-元素对
	// 若键已存在,新元素会替换旧的元素值
	Put(key string, element interface{}) (bool, error)
	// Get 获取与指定关联的那个元素
	// 若返回nil, 则说明指定的键不存在
	Get(key string) interface{}
	// Delete 删除指定的键-元素对
	// 若结果值为true则说明键已存在且已删除,否则说明键不存在
	Delete(key string) bool
	// Len 返回当前字典中键-元素对的数量
	Len() uint64
	// ForEach 迭代器
	ForEach(fn func(key string, value interface{}))
}

// myConcurrentMap 代表ConcurrencyMap接口的实现类型
type myConcurrentMap struct {
	concurrency int
	segments    []Segment
	total       uint64
}

// NewConcurrentMap 创建一个Concurrent类型的实例
// 参数pairRedistributor可以为nil
func NewConcurrentMap(concurrency int, pairRedistributor PairRedistributor) (ConcurrentMap, error) {
	if concurrency <= 0 {
		return nil, newIllegalParameterError("concurrency is too small")
	}
	if concurrency > MAX_CONCURRENCY {
		return nil, newIllegalParameterError("concurrency is too large")
	}
	cmap := &myConcurrentMap{}
	cmap.concurrency = concurrency
	cmap.segments = make([]Segment, concurrency)
	for i := 0; i < concurrency; i++ {
		cmap.segments[i] = newSegment(DEFAULT_BUCKET_NUMBER, pairRedistributor)
	}
	return cmap, nil
}

// Concurrency 返回并发量
func (cmap *myConcurrentMap) Concurrency() int {
	return cmap.concurrency
}

// Put  推送一个键-元素对
// 注意!参数element的值不能为nil
// 第一个返回值表示是否新增了键-元素对
// 若键已存在,新元素会替换旧的元素值
func (cmap *myConcurrentMap) Put(key string, element interface{}) (bool, error) {
	p, err := newPair(key, element)
	if err != nil {
		return false, err
	}
	s := cmap.findSegment(p.Hash())
	ok, err := s.Put(p)
	if ok {
		atomic.AddUint64(&cmap.total, 1)
	}
	return ok, err
}

// Get 获取与指定关联的那个元素
// 若返回nil, 则说明指定的键不存在
func (cmap *myConcurrentMap) Get(key string) interface{} {
	keyHash := hash(key)
	s := cmap.findSegment(keyHash)
	pair := s.GetWithHash(key, keyHash)
	if pair == nil {
		return nil
	}
	return pair.Element()
}

// Delete 删除指定的键-元素对
// 若结果值为true则说明键已存在且已删除,否则说明键不存在
func (cmap *myConcurrentMap) Delete(key string) bool {
	s := cmap.findSegment(hash(key))
	if s.Delete(key) {
		atomic.AddUint64(&cmap.total, ^uint64(0))
		return true
	}
	return false
}

// Len 返回当前字典中键-元素对的数量
func (cmap *myConcurrentMap) Len() uint64 {
	return atomic.LoadUint64(&cmap.total)
}

// ForEach 迭代器
func (cmap *myConcurrentMap) ForEach(fn func(key string, value interface{})) {
	if fn != nil {
		for i := 0; i < int(cmap.Concurrency()); i++ {
			cmap.segments[i].ForEach(fn)
		}
	}
}

// findSegment 根据给定参数寻找并返回对应散列字段
func (cmap *myConcurrentMap) findSegment(keyHash uint64) Segment {
	if cmap.concurrency == 1 {
		return cmap.segments[0]
	}
	var keyHash32 uint32
	if keyHash > math.MaxUint32 {
		keyHash32 = uint32(keyHash32 >> 32)
	} else {
		keyHash32 = uint32(keyHash32)
	}
	return cmap.segments[int(keyHash32>>16)%(cmap.concurrency-1)]
}
