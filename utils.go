package cmap

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"log"
)

// hash 计算给定字符串的哈希值的整数形式(BKDR哈希算法)
func hash(str string) uint64 {
	seed := uint64(13131)
	var hash uint64
	for i := 0; i < len(str); i++ {
		hash = hash*seed + uint64(str[i])
	}
	return hash & 0x7FFFFFFFFFFFFFFF
}

// hash2 计算字符串哈希
func hash2(str string) uint64 {
	h := md5.Sum([]byte(str))
	var num uint64
	_ = binary.Read(bytes.NewReader(h[:]), binary.LittleEndian, &num)
	return num
}

var DEBUG = false

// logMsg 打印信息
func logMsg(format string, v ...interface{}) {
	if DEBUG {
		log.Printf(format, v...)
	}
}
