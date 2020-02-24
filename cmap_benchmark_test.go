package cmap

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
)

// -- Put -- //

func BenchmarkCmapPutAbsent(b *testing.B) {
	var number = 20
	var testCases = genNoRepetitiveTestingPairs(number)
	concurrency := number / 4
	cm, _ := NewConcurrentMap(concurrency, nil)
	b.ResetTimer()
	for _, tc := range testCases {
		key := tc.Key()
		element := tc.Element()
		b.Run(key, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = cm.Put(key, element)
			}
		})
	}
}

func BenchmarkCmapPutPresent(b *testing.B) {
	var number = 20
	concurrency := number / 4
	cm, _ := NewConcurrentMap(concurrency, nil)
	key := "invariable key"
	b.ResetTimer()
	for i := 0; i < number; i++ {
		element := strconv.Itoa(i)
		b.Run(key, func(b *testing.B) {
			for j := 0; j < b.N; j++ {
				_, _ = cm.Put(key, element)
			}
		})
	}
}

func BenchmarkMapPut(b *testing.B) {
	var number = 10
	var testCases = genNoRepetitiveTestingPairs(number)
	m := make(map[string]interface{})
	b.ResetTimer()
	for _, tc := range testCases {
		key := tc.Key()
		element := tc.Element()
		b.Run(key, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				m[key] = element
			}
		})
	}
}

// -- Get -- //

func BenchmarkCmapGet(b *testing.B) {
	var number = 100000
	var testCases = genNoRepetitiveTestingPairs(number)
	concurrency := number / 4
	cm, _ := NewConcurrentMap(concurrency, nil)
	for _, p := range testCases {
		_, _ = cm.Put(p.Key(), p.Element())
	}
	b.ResetTimer()
	for i := 0; i < 10; i++ {
		key := testCases[rand.Intn(number)].Key()
		b.Run(key, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = cm.Get(key)
			}
		})
	}
}

func BenchmarkMapGet(b *testing.B) {
	var number = 100000
	var testCases = genNoRepetitiveTestingPairs(number)
	m := make(map[string]interface{})
	for _, p := range testCases {
		m[p.Key()] = p.Element()
	}
	b.ResetTimer()
	for i := 0; i < 10; i++ {
		key := testCases[rand.Intn(number)].Key()
		b.Run(key, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = m[key]
			}
		})
	}
}

// -- Delete -- /

func BenchmarkMarkCmapDelete(b *testing.B) {
	var number = 100000
	var testCases = genNoRepetitiveTestingPairs(number)
	concurrency := number / 4
	cm, _ := NewConcurrentMap(concurrency, nil)
	for _, p := range testCases {
		_, _ = cm.Put(p.Key(), p.Element())
	}
	b.ResetTimer()
	for i := 0; i < 20; i++ {
		key := testCases[rand.Intn(number)].Key()
		b.Run(key, func(b *testing.B) {
			cm.Delete(key)
		})
	}
}

func BenchmarkMapDelete(b *testing.B) {
	var number = 100000
	var testCases = genNoRepetitiveTestingPairs(number)
	m := make(map[string]interface{})
	for _, p := range testCases {
		m[p.Key()] = p.Element()
	}
	b.ResetTimer()
	for i := 0; i < 20; i++ {
		key := testCases[rand.Intn(number)].Key()
		b.Run(key, func(b *testing.B) {
			delete(m, key)
		})
	}
}

// -- Len -- //

func BenchmarkCmapLen(b *testing.B) {
	var number = 100000
	var testCases = genNoRepetitiveTestingPairs(number)
	concurrency := number / 4
	cm, _ := NewConcurrentMap(concurrency, nil)
	for _, p := range testCases {
		_, _ = cm.Put(p.Key(), p.Element())
	}
	b.ResetTimer()
	for i := 0; i < 5; i++ {
		b.Run(fmt.Sprintf("Len%d", i), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				cm.Len()
			}
		})
	}
}

func BenchmarkMapLen(b *testing.B) {
	var number = 100000
	var testCases = genNoRepetitiveTestingPairs(number)
	m := make(map[string]interface{})
	for _, p := range testCases {
		m[p.Key()] = p.Element()
	}
	b.ResetTimer()
	for i := 0; i < 5; i++ {
		b.Run(fmt.Sprintf("Len%d", i), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = len(m)
			}
		})
	}
}

// -- ForEach -- //

func BenchmarkCmapForEach(b *testing.B) {
	var number = 100000
	var testCases = genNoRepetitiveTestingPairs(number)
	concurrency := number / 4
	cm, _ := NewConcurrentMap(concurrency, nil)
	for _, p := range testCases {
		_, _ = cm.Put(p.Key(), p.Element())
	}
	b.ResetTimer()
	for i := 0; i < 5; i++ {
		b.Run(fmt.Sprintf("ForEach%d", i), func(b *testing.B) {
			cm.ForEach(func(key string, value interface{}) {
				_, _ = key, value
			})
		})
	}
}

func BenchmarkMapRange(b *testing.B) {
	var number = 100000
	var testCases = genNoRepetitiveTestingPairs(number)
	m := make(map[string]interface{})
	for _, p := range testCases {
		m[p.Key()] = p.Element()
	}
	b.ResetTimer()
	for i := 0; i < 5; i++ {
		b.Run(fmt.Sprintf("Range%d", i), func(b *testing.B) {
			for k, v := range m {
				_, _ = k, v
			}
		})
	}
}
