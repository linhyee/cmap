package cmap

import (
	"sync/atomic"
)

// BucketStatus 代表散列桶状态的类型
type BucketStatus uint8

const (
	// BUCKET_STATUS_NORMAL 代表散列桶正常
	BUCKET_STATUS_NORMAL BucketStatus = 0
	// BUCKET_STATUS_UNDERWEIGHT 代表散列桶过轻
	BUCKET_STATUS_UNDERWEIGHT BucketStatus = 1
	// BUCKET_STATUS_OVERWEIGHT 代表散列桶过重
	BUCKET_STATUS_OVERWEIGHT BucketStatus = 2
)

//
//
type PairRedistributor interface {
	// UpdateThreshold 根据键-元素对总数和散列桶总数计算并更新阈值
	UpdateThreshold(pairTotal uint64, bucketNumber int)
	// CheckBucketStatus 用于检查散列桶的状态
	CheckBucketStatus(pairTotal uint64, bucketSize uint64) (bucketStatus BucketStatus)
	// Redistribe 用于实施键-元素对的再分布
	Redistribe(bucketStatus BucketStatus, buckets []Bucket) (newBuckets []Bucket, changed bool)
}

// myPairRedistributor 代表PairRedistributor的默认实现类型
type myPairRedistributor struct {
	// loadFactor 代表装载因子
	loadFactor float64
	// upperThreshold 代表散列桶重量的上阈值
	// 当某个散列桶的尺寸增至此值时会触发再散列
	upperThreshold uint64
	// overweightBucketCount 代表过重的散列桶的计数
	overweightBucketCount uint64
	// emptyBucketCount 代表空的散列桶的计数
	emptyBucketCount uint64
}

// newDefaultPairRedistributor 创建一个PairRedistributor类型的实例
// 参数loadFactor代表散列桶的负载因子
// 参数bucketNumber代表散列桶的数量
func newDefaultPairRedistributor(loadFactor float64, bucketNumber int) PairRedistributor {
	if loadFactor <= 0 {
		loadFactor = DEFAULT_BUCKET_LOAD_FACTOR
	}
	pr := &myPairRedistributor{}
	pr.loadFactor = loadFactor
	pr.UpdateThreshold(0, bucketNumber)
	return pr
}

// bucketCountTemplate 代表调试用散列桶状态信息模板
var bucketCountTemplate = `Bucket count:
	pairTotal: %d
	bucketNumber: %d
	average: %f
	upperThreshold: %d
	emptyBucketCount: %d
`

// UpdateThreshold 根据键-元素对总数和散列桶总数计算并更新阈值
func (pr *myPairRedistributor) UpdateThreshold(pairTotal uint64, bucketNumber int) {
	var average float64
	average = float64(pairTotal / uint64(bucketNumber))
	if average < 100 {
		average = 100
	}
	defer func() {
		logMsg(bucketCountTemplate, pairTotal, bucketNumber, average,
			atomic.LoadUint64(&pr.upperThreshold), atomic.LoadUint64(&pr.emptyBucketCount))
	}()
	atomic.StoreUint64(&pr.upperThreshold, uint64(average*pr.loadFactor))
}

var bucketStatusTemplate = `Check bucket status:
	pairTotal: %d
	bucketSize: %d
	upperThreshold: %d
	overweightBucketCount: %d
	emptyBucketCount: %d
	bucketStatus: %d
`

// CheckBucketStatus 用于检查散列桶的状态
func (pr *myPairRedistributor) CheckBucketStatus(pairTotal uint64, bucketSize uint64) (bucketStatus BucketStatus) {
	defer func() {
		logMsg(bucketStatusTemplate, pairTotal, bucketSize, atomic.LoadUint64(&pr.upperThreshold),
			atomic.LoadUint64(&pr.overweightBucketCount), atomic.LoadUint64(&pr.emptyBucketCount), bucketStatus)
	}()
	if bucketSize > DEFAULT_BUCKET_MAX_SIZE || bucketSize >= atomic.LoadUint64(&pr.upperThreshold) {
		atomic.AddUint64(&pr.overweightBucketCount, 1)
		bucketStatus = BUCKET_STATUS_OVERWEIGHT
		return
	}
	if bucketSize == 0 {
		atomic.AddUint64(&pr.emptyBucketCount, 1)
	}
	return
}

// redistributionTemplate 代表重新分配信息模板
var redistributionTemplate = `Redistributing:
	bucketStatus: %d
	bucketNumber: %d
	newNumber: %d
`

// Redistribe 用于实施键-元素对的再分布
func (pr *myPairRedistributor) Redistribe(bucketStatus BucketStatus, buckets []Bucket) (newBuckets []Bucket, changed bool) {
	currentNumber := uint64(len(buckets))
	newNumber := currentNumber
	defer func() {
		logMsg(redistributionTemplate, bucketStatus, currentNumber, newNumber)
	}()
	//扩张或裁减散桶的大小
	switch bucketStatus {
	case BUCKET_STATUS_OVERWEIGHT:
		if atomic.LoadUint64(&pr.overweightBucketCount)*4 < currentNumber {
			return nil, false
		}
		newNumber = currentNumber << 1
	case BUCKET_STATUS_UNDERWEIGHT:
		if currentNumber < 100 || atomic.LoadUint64(&pr.emptyBucketCount)*4 < currentNumber {
			return nil, false
		}
		newNumber = currentNumber >> 1
		if newNumber < 2 {
			newNumber = 2
		}
	default:
		return nil, false
	}
	//经过计算,如果相等就不必要操作
	if newNumber == currentNumber {
		atomic.StoreUint64(&pr.overweightBucketCount, 0)
		atomic.StoreUint64(&pr.emptyBucketCount, 0)
		return nil, false
	}
	//重新分配键-元素对
	var pairs []Pair
	//复制到副本
	for _, b := range buckets {
		for e := b.GetFirstPair(); e != nil; e = e.Next() {
			pairs = append(pairs, e)
		}
	}
	//清空所有的原散列桶
	if newNumber > currentNumber {
		for i := uint64(0); i < currentNumber; i++ {
			buckets[i].Clear(nil)
		}
		//扩展新桶
		for j := newNumber - currentNumber; j > 0; j-- {
			buckets = append(buckets, newBucket())
		}
	} else {
		//裁减原桶
		buckets = make([]Bucket, newNumber)
		for i := uint64(0); i < newNumber; i++ {
			buckets[i] = newBucket()
		}
	}
	var count int
	//k-v对重新分配到各桶
	for _, p := range pairs {
		index := int(p.Hash() % newNumber)
		b := buckets[index]
		_, _ = b.Put(p, nil)
		count++
	}
	atomic.StoreUint64(&pr.overweightBucketCount, 0)
	atomic.StoreUint64(&pr.emptyBucketCount, 0)
	return buckets, true
}
