package cmap

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"unsafe"
)

// linkedPair 代表单向链接的键-元素对接口
type linkedPair interface {
	// Next 用于获得下一个键-元素对
	// 若返回值为nil,则说明当前已在单链表的末尾
	Next() Pair
	// SetNext 用于设置一个键-元素对
	// 这样就可以形成一个键-元素对的单键表
	SetNext(nextPair Pair) error
}

// Pair 代表并发安全的键-元素对的接口
type Pair interface {
	// linkedPair 代表单链键-元素对接口
	linkedPair
	// Key 返回键的值
	Key() string
	// Hash 返回键的哈希值
	Hash() uint64
	// Element 返回元素的值
	Element() interface{}
	// SetElement 设置元素的值
	SetElement(element interface{}) error
	// Copy 生成一个当前键-元素对的副本并返回
	Copy() Pair
	// String 返回当前键-元素对的字符串表示形式
	String() string
}

// pair 代表键-元素对的类型
type pair struct {
	key     string
	hash    uint64 //代表键的哈希值
	element unsafe.Pointer
	next    unsafe.Pointer
}

// newPair 创建一个Pair类型的实例
func newPair(key string, element interface{}) (Pair, error) {
	p := &pair{key: key, hash: hash(key)}
	if element == nil {
		return nil, newIllegalParameterError("element is nil")
	}
	p.element = unsafe.Pointer(&element)
	return p, nil
}

// Key 返回键的值
func (p *pair) Key() string {
	return p.key
}

// Hash 返回键的哈希值
func (p *pair) Hash() uint64 {
	return p.hash
}

// Element 返回元素的值
func (p *pair) Element() interface{} {
	pointer := atomic.LoadPointer(&p.element)
	if pointer == nil {
		return nil
	}
	return *(*interface{})(pointer)
}

// SetElement 设置元素的值
func (p *pair) SetElement(element interface{}) error {
	if element == nil {
		return newIllegalParameterError("element is nil")
	}
	atomic.StorePointer(&p.element, unsafe.Pointer(&element))
	return nil
}

// Next 用于获得下一个键-元素对
// 若返回值为nil,则说明当前已在单链表的末尾
func (p *pair) Next() Pair {
	pointer := atomic.LoadPointer(&p.next)
	if pointer == nil {
		return nil
	}
	return (*pair)(pointer)
}

// SetNext 用于设置一个键-元素对
// 这样就可以形成一个键-元素对的单键表
func (p *pair) SetNext(nextPair Pair) error {
	if nextPair == nil {
		atomic.StorePointer(&p.next, nil)
	}
	pp, ok := nextPair.(*pair)
	if !ok {
		return newIllegalPairTypeError(nextPair)
	}
	atomic.StorePointer(&p.next, unsafe.Pointer(pp))
	return nil
}

// Copy 生成一个当前键-元素对的副本并返回
func (p *pair) Copy() Pair {
	pCopy, _ := newPair(p.key, p.Element())
	return pCopy
}

// String 返回当前键-元素对的字符串表示形式
func (p *pair) String() string {
	return p.genString(false)
}

// genString 用于生成并返回当前键-元素对的字符形式
func (p *pair) genString(nextDetail bool) string {
	var buf bytes.Buffer
	buf.WriteString("pair{key:")
	buf.WriteString(p.Key())
	buf.WriteString(",hash:")
	buf.WriteString(fmt.Sprintf("%d", p.Hash()))
	buf.WriteString(", element:")
	buf.WriteString(fmt.Sprintf("%+v", p.Element()))
	if nextDetail {
		buf.WriteString(", next:")
		if next := p.Next(); next != nil {
			if npp, ok := next.(*pair); ok {
				buf.WriteString(npp.genString(nextDetail))
			} else {
				buf.WriteString("<ignore>")
			}
		}
	} else {
		buf.WriteString(", nextKey:")
		if next := p.Next(); next != nil {
			buf.WriteString(next.Key())
		}
	}
	buf.WriteString("}")
	return buf.String()
}
