package dm

import (
    "fmt"
    "reflect"
    "unsafe"

    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/container/cmap"
    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/convert"
    memory_tile "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/encoding/memory-tile"
)

type NogcMap struct {
    BaseStore

    cMap *cmap.CMap // cmap
}

func NewNogcMap(seed ...interface{}) *NogcMap {
    // 由于DataManager中有事务锁
    // 所以CMap可以使用非并发安全的模式
    cmapConfig := cmap.DefaultConfig
    cmapConfig.Concurrent = false

    p := &NogcMap{
        cMap:  cmap.NewCMap(cmapConfig),
    }
    p.SetSeed(seed...)
    return p
}

// Size 获取数据数量
func (n *NogcMap) Size() int {
    return int(n.cMap.Length())
}

// HasKey 检测key是否存在
func (n *NogcMap) HasKey(key KeyType) bool {
    return n.cMap.HasKey(key)
}

// Keys 返回key的列表
func (n *NogcMap) Keys() []KeyType {
    return n.cMap.Keys()
}

// Get 通过key获取value
// any的类型与SetSeed中传入的类型保持一致
func (n *NogcMap) Get(key KeyType) (any interface{}, ret error) {
    v, err := n.NewObjectBySeed()
    if err != nil {
        return nil, err
    }

    i := v.Interface()
    if err := n.Fetch(key, i); err != nil {
        return nil, err
    }

    if n.isPtr {
        return i, nil
    }
    return v.Elem().Interface(), nil
}

// GetUnsafe 通过key获取value,非安全方式，持有C侧数据指针
func (n *NogcMap) GetUnsafe(key KeyType) (value unsafe.Pointer, err error) {
    data, err := n.cMap.GetUnsafe(key)
    if err != nil {
       if err == cmap.ErrEntryNotFound {
           return nil, KeyNotExists
       }
       return nil, err
    }
    // Assert选用非调整模式，不会更改内存数据，因为get方式外部使用的读锁，所以不能修改内存数据
    return memory_tile.Assert(data, false), nil
}

// Set 设置key, value
func (n *NogcMap) Set(key KeyType, input interface{}) error {
    value := input

    // 指针 -> 指向内容
    typ := reflect.TypeOf(input)
    if typ.Kind() == reflect.Ptr {
        value = reflect.ValueOf(value).Elem().Interface()
    }

    // data的底层内存空间分配在C内存空间中
    data, err := memory_tile.MarshalC(value)
    if err != nil {
        return err
    }

    // 数据种子保持input的类型
    // 此处保障效率, 并未保障SetSeed的并发安全
    if n.NeedSetSeed() {
        n.SetSeed(input)
    }

    // 因为value使用MarshalC -> 生成C内存空间 -> 使用SetUnsafe直接设置指针, 减少内存拷贝
    return n.cMap.SetUnsafe(key, data)
}

// Del 删除key对应的value
func (n *NogcMap) Del(key KeyType) (has bool) {
    return n.cMap.Delete(key)
}

// Fetch 获取key对应的value的拷贝
func (n *NogcMap) Fetch(key KeyType, ptr interface{}) error {
    data, err := n.cMap.Get(key)
    if err != nil {
        if err == cmap.ErrEntryNotFound {
            return KeyNotExists
        }
        return err
    }

    // Get的data已经进行了拷贝 -> 使用UnmarshalNocopy
    if err := memory_tile.UnmarshalNocopy(data, ptr); err != nil {
        return err
    }
    return nil
}

// Iterator 迭代遍历
func (n *NogcMap) Iterator(iter func(keyType KeyType, value interface{}) bool) error {
    if iter == nil {
        return fmt.Errorf("input iter is nil")
    }

    for i := n.cMap.Iterator(); i.Next(); {
        key, value, err := i.Get()
        if err == cmap.ErrEntryNotFound {
            continue
        }
        if err != nil {
            return err
        }

        v, e := n.NewObjectBySeed()
        if e != nil {
            return e
        }

        i := v.Interface()
        if err := memory_tile.UnmarshalNocopy(value, i); err != nil {
            return err
        }

        c := false
        if n.isPtr {
            c = iter(key, i)
        } else {
            c = iter(key, v.Elem().Interface())
        }

        // iter返回值决定是否继续循环遍历
        if !c {
            break
        }
    }
    return nil
}

// Lookup 快速查看对象
func (n *NogcMap) Lookup(key KeyType, safe ...bool) (pointer unsafe.Pointer, ret error) {
    defer func() {
        if err := recover(); err != nil {
            ret = convert.Error(err)
        }
    }()

    var data []byte

    // 数据是否安全 -> 默认安全
    dataSafe := true
    if len(safe) > 0 {
        dataSafe = safe[0]
    }

    if dataSafe {
        // Get返回的数据进行了拷贝
        data, ret = n.cMap.Get(key)
    } else {
        // GetUnsafe返回的数据未进行拷贝, 可能在使用过程中被修改或删除
        data, ret = n.cMap.GetUnsafe(key)
    }

    if ret != nil {
        return
    }

    pointer = memory_tile.Assert(data)
    return
}

// Capacity 获取内部数据存储容量
func (n *NogcMap) Capacity() int {
    return n.cMap.Capacity()
}
