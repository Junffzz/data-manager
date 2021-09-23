package dm

import (
    "fmt"
    "testing"

    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/utils"
)

func TestKeyLatestStore_Set(t *testing.T) {
    type A struct {
        X int
        Y int
        Z string
    }

    limit := 5
    size := 20

    store := NewKeyLatestStore(ENGINE_NOGC_MAP, A{}, limit)
    keys := make([]KeyType, 0, size)
    for i := 0; i < size; i++ {
        keys = append(keys, i)
    }

    // ID打乱
    shuffle := utils.SliceReverse(keys).([]int)
    fmt.Printf("shuffle:%+v\n", shuffle)

    for _, v := range shuffle {
        a := A{
            X: v,
            Y: v + 1,
            Z: fmt.Sprintf("A%d", v),
        }
        err := store.Set(v, &a)
        if err != nil {
            panic(err)
        }
    }
    fmt.Printf("store size:%+v\n", store.Size())

    err := store.Iterator(func(key KeyType, value interface{}) bool {
        fmt.Printf("key:%d %+v\n", key, value)
        return true
    })
    if err != nil {
        panic(err)
    }
}

func TestKeyLatestStore_Set_Shuffle(t *testing.T) {
    type A struct {
        X int
        Y int
        Z string
    }

    limit := 5
    size := 20

    store := NewKeyLatestStore(ENGINE_NOGC_MAP, A{}, limit)
    keys := make([]KeyType, 0, size)
    for i := 0; i < size; i++ {
        keys = append(keys, i)
    }

    // ID打乱
    shuffle := utils.SliceShuffle(keys)
    fmt.Printf("shuffle:%+v\n", shuffle)

    for _, item := range shuffle {
        v := item.(KeyType)
        a := A{
            X: v,
            Y: v + 1,
            Z: fmt.Sprintf("A%d", v),
        }
        err := store.Set(v, &a)
        if err != nil {
            panic(err)
        }
    }
    fmt.Printf("store size:%+v\n", store.Size())

    err := store.Iterator(func(key KeyType, value interface{}) bool {
        fmt.Printf("key:%d %+v\n", key, value)
        return true
    })
    if err != nil {
        panic(err)
    }
}