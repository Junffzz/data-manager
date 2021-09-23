package dm

import (
    "fmt"
    "reflect"
)

// BaseStore 基础的IStore实现
type BaseStore struct {
    seed  reflect.Type  // seed的类型 (如果为指针, 保存其Elem的类型)
    isPtr bool          // seed是否为指针
}

func (n *BaseStore) NewObjectBySeed() (reflect.Value, error) {
    if n.seed == nil {
        return reflect.Value{}, fmt.Errorf("BaseStore seed is nil")
    }

    return reflect.New(n.seed), nil
}

// SetSeed 设置数据类型的种子
// Get 根据种子生成对应类型的实例
func (n *BaseStore) SetSeed(seeds ...interface{}) {
    if len(seeds) > 0 && seeds[0] != nil {
        n.setSeed(seeds[0])
    }
}

// SeedSize 获取种子的Size
func (n *BaseStore) SeedSize() int {
    if n.seed != nil {
        return int(n.seed.Size())
    }
    return 0
}

// NeedSetSeed 判断是否需要设置种子
func (n *BaseStore) NeedSetSeed() bool {
    return n.seed == nil
}

// IsPtr 获取种子是否为指针类型
func (n *BaseStore) IsPtr() bool {
    return n.isPtr
}

func (n *BaseStore) setSeed(seed interface{}) {
    n.seed = reflect.TypeOf(seed)
    if n.seed.Kind() == reflect.Ptr {
        n.seed = n.seed.Elem()
        n.isPtr = true
    }
}