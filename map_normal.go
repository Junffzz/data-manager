package dm

import (
    "fmt"

    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/utils"
)

type engineType = map[DataIndex]interface{}

type NormalMap struct {
    BaseStore

    engine engineType // 底层存储
}

func NewNormalMap(seed ...interface{}) *NormalMap {
    p := &NormalMap{
        engine: make(engineType),
    }
    p.SetSeed(seed...)
    return p
}

// Size 获取数据数量
func (n *NormalMap) Size() int {
    return len(n.engine)
}

// HasKey 检测key是否存在
func (n *NormalMap) HasKey(key KeyType) bool {
    _, ok := n.engine[key]
    return ok
}

// Keys 返回key的列表
func (n *NormalMap) Keys() []KeyType {
    keys := make([]KeyType, 0, len(n.engine))
    for k, _ := range n.engine {
        keys = append(keys, k)
    }
    return keys
}

// Get 通过key获取value
func (n *NormalMap) Get(key KeyType) (value interface{}, err error) {
    v, ok := n.engine[key]
    if !ok {
        return nil, KeyNotExists
    }
    return v, nil
}

// Set 设置key, value
func (n *NormalMap) Set(key KeyType, value interface{}) error {
    n.engine[key] = value

    // 数据种子保持input的类型
    // 此处保障效率, 并未保障SetSeed的并发安全
    if n.NeedSetSeed() {
        n.SetSeed(value)
    }
    return nil
}

// Del 删除key对应的value
func (n *NormalMap) Del(key KeyType) (has bool) {
    if _, has = n.engine[key]; has {
        delete(n.engine, key)
    }
    return
}

// Fetch 获取key对应的value的拷贝
func (n *NormalMap) Fetch(key KeyType, valuePtr interface{}) error {
    value, err := n.Get(key)
    if err != nil {
        return err
    }

    if err := utils.Clone(value, valuePtr); err != nil {
        return err
    }
    return nil
}

// Iterator 迭代遍历
func (n *NormalMap) Iterator(iter func(keyType KeyType, value interface{}) bool) error {
    if iter == nil {
        return fmt.Errorf("input iter is nil")
    }

    for k, v := range n.engine {
        if c := iter(k, v); !c {
            break
        }
    }
    return nil
}

// Capacity 获取内部数据存储容量
// NormalMap 只计算结构的大小, 忽略引用字段 (<= 实际内存)
func (n *NormalMap) Capacity() int {
    return len(n.engine) * n.SeedSize()
}
