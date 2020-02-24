package cmap

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Segment 代表并发安全的散列段的接口
type Segment interface {
	// Put 根据参数放入一个键-元素对
	// 第一个返回值表示是否新增了键-元素对
	Put(p Pair) (bool, error)
	// Get 根据给定参数返回对应的键-元素对
	Get(key string) Pair
	// GetWithHash 根据给定参数返回对应的键-元素对
	// 注意!参数keyHash应该是基于参数key计算得出哈希值
	GetWithHash(key string, keyHash uint64) Pair
	// Delete 删除指定键的键-元素对
	// 若返回值为true则说明已删除,否则说明未找到该键
	Delete(key string) bool
	// Size 用于获取当前段的尺寸 (其中包含的散列桶的数量)
	Size() uint64
	// ForEach 迭代当前段的键-元素对
	ForEach(fn func(key string, value interface{}))
}

// segment 代表并发安全的散列段的类型
type segment struct {
	// buckets 代表散列桶切片
	buckets []Bucket
	// bucketsLen 代表散列桶切片的长度
	bucketsLen int
	// pairTotal 代表键-元素对总数
	pairTotal uint64
	// pairRedistributor 代表键-元素的再分布器
	pairRedistributor PairRedistributor
	// lock 保护段的互斥锁
	// 任时候只有一个Goroutine能对段进行写操作
	lock sync.Mutex
}

// newSegment 创建一个Segment类型的实例
func newSegment(bucketNumber int, pairRedistributor PairRedistributor) Segment {
	if bucketNumber <= 0 {
		bucketNumber = DEFAULT_BUCKET_NUMBER
	}
	if pairRedistributor == nil {
		pairRedistributor = newDefaultPairRedistributor(DEFAULT_BUCKET_LOAD_FACTOR, bucketNumber)
	}
	buckets := make([]Bucket, bucketNumber)
	for i := 0; i < bucketNumber; i++ {
		buckets[i] = newBucket()
	}
	return &segment{
		buckets:           buckets,
		bucketsLen:        bucketNumber,
		pairRedistributor: pairRedistributor,
	}
}

// Put 根据参数放入一个键-元素对
// 第一个返回值表示是否新增了键-元素对
func (s *segment) Put(p Pair) (bool, error) {
	s.lock.Lock()
	b := s.buckets[int(p.Hash()%uint64(s.bucketsLen))]
	ok, err := b.Put(p, nil)
	if ok {
		newTotal := atomic.AddUint64(&s.pairTotal, 1)
		_ = s.redistribute(newTotal, b.Size())
	}
	s.lock.Unlock()
	return ok, err
}

// Get 根据给定参数返回对应的键-元素对
func (s *segment) Get(key string) Pair {
	return s.GetWithHash(key, hash(key))
}

// GetWithHash 根据给定参数返回对应的键-元素对
// 注意!参数keyHash应该是基于参数key计算得出哈希值
func (s *segment) GetWithHash(key string, keyHash uint64) Pair {
	s.lock.Lock()
	b := s.buckets[int(keyHash%uint64(s.bucketsLen))]
	s.lock.Unlock()
	return b.Get(key)
}

// Delete 删除指定键的键-元素对
// 若返回值为true则说明已删除,否则说明未找到该键
func (s *segment) Delete(key string) bool {
	s.lock.Lock()
	b := s.buckets[int(hash(key)%uint64(s.bucketsLen))]
	ok := b.Delete(key, nil)
	if ok {
		newTotal := atomic.AddUint64(&s.pairTotal, ^uint64(0))
		_ = s.redistribute(newTotal, b.Size())
	}
	s.lock.Unlock()
	return ok
}

// Size 用于获取当前段的尺寸 (其中包含的散列桶的数量)
func (s *segment) Size() uint64 {
	return atomic.LoadUint64(&s.pairTotal)
}

// ForEach 迭代当前段的键-元素对
func (s *segment) ForEach(fn func(key string, value interface{})) {
	if fn == nil {
		return
	}
	s.lock.Lock()
	for i := 0; i < s.bucketsLen; i++ {
		for v := s.buckets[i].GetFirstPair(); v != nil; v = v.Next() {
			fn(v.Key(), v.Element())
		}
	}
	s.lock.Unlock()
}

// redistribute 检查给定参数并设置相应的阈值和计数
// 并在必要时重新分配所有散列桶中的所有键-元素对
// 注意!必须在互斥锁的保护下调用本方法
func (s *segment) redistribute(pairTotal uint64, bucketSize uint64) (err error) {
	defer func() {
		// 再分配器有可能是第三方外部注入组件,所以这里要进行恐慌处理
		if p := recover(); p != nil {
			if pErr, ok := p.(error); ok {
				err = newPairRedistributorError(pErr.Error())
			} else {
				err = newPairRedistributorError(fmt.Sprintf("%s", p))
			}
		}
	}()
	s.pairRedistributor.UpdateThreshold(pairTotal, s.bucketsLen)
	bucketStatus := s.pairRedistributor.CheckBucketStatus(pairTotal, bucketSize)
	newBuckets, change := s.pairRedistributor.Redistribe(bucketStatus, s.buckets)
	if change {
		s.buckets = newBuckets
		s.bucketsLen = len(s.buckets)
	}
	return nil
}

// String 返回当前segment字符串表示形式
func (s *segment) String() string {
	var buf bytes.Buffer
	buf.WriteString("bucketsLen: ")
	buf.WriteString(fmt.Sprintf("%d, ", s.bucketsLen))
	buf.WriteString("pairTotal: ")
	buf.WriteString(fmt.Sprintf("%d, ", s.pairTotal))
	buf.WriteString("buckets info:\n")
	for i := 0; i < int(atomic.LoadInt32((*int32)(unsafe.Pointer(&s.bucketsLen)))); i++ {
		if i > 0 {
			buf.WriteString("\n")
		}
		buf.WriteString(fmt.Sprintf("\t%2d:", i))
		buf.WriteString(s.buckets[i].String())
	}
	return buf.String()
}
