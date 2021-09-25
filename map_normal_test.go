package dm

import (
    "fmt"
    "testing"

    memory_tile "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/encoding/memory-tile"
)

// BenchmarkNogcMap_Set-8     155323      9158 ns/op   10055 B/op       6 allocs/op
func BenchmarkNormalMap_Set(b *testing.B) {
    b.ReportAllocs()

    big := memory_tile.GenBigStruct(0)
    m := NewNormalMap()
    id := 0
    for i := 0; i < b.N; i++ {
        big.Id = i

        if err := m.Set(id, big); err != nil {
            panic(err)
        }
        id++
    }

    if size := m.Size(); size != b.N {
        panic(size)
    }
}

// BenchmarkNogcMap_Get-8     613087      2042 ns/op    6656 B/op       3 allocs/op
func BenchmarkNormalMap_Get(b *testing.B) {
    big := memory_tile.GenBigStruct(0)

    m := NewNormalMap()

    id := 0
    for i := 0; i < b.N; i++ {
        big.Id = i

        if err := m.Set(id, big); err != nil {
            panic(err)
        }
        id++
    }

    b.ReportAllocs()
    b.ResetTimer()

    id = 0
    for i := 0; i < b.N; i++ {
        l, err := m.Get(id)
        if err != nil {
            panic(fmt.Errorf("id:%d err:%v", id, err))
        }

        ll := l.(memory_tile.Lecture)
        if ll.Id != id {
            panic(l)
        }

        id++
    }
}

// BenchmarkNogcMap_Get1-8     671212      1609 ns/op    5376 B/op       2 allocs/op
func BenchmarkNormalMap_Get_Ptr(b *testing.B) {
    big := memory_tile.GenBigStruct(0)

    m := NewNormalMap()

    id := 0
    for i := 0; i < b.N; i++ {
        big.Id = i

        if err := m.Set(id, &big); err != nil {
            panic(err)
        }
        id++
    }

    b.ReportAllocs()
    b.ResetTimer()

    id = 0
    for i := 0; i < b.N; i++ {
        l, err := m.Get(id)
        if err != nil {
            panic(fmt.Errorf("id:%d err:%v", id, err))
        }

        ll := l.(*memory_tile.Lecture)
        if ll.Id != id {
            panic(l)
        }

        id++
    }
}

// BenchmarkNogcMap_Fetch-8     758998      1662 ns/op    5376 B/op       2 allocs/op
func BenchmarkNormalMap_Fetch(b *testing.B) {
    big := memory_tile.GenBigStruct(0)

    m := NewNormalMap()

    id := 0
    for i := 0; i < b.N; i++ {
        big.Id = i

        if err := m.Set(id, big); err != nil {
            panic(err)
        }
        id++
    }

    b.ReportAllocs()
    b.ResetTimer()

    id = 0
    for i := 0; i < b.N; i++ {
        var l memory_tile.Lecture
        err := m.Fetch(id, &l)
        if err != nil {
            panic(fmt.Errorf("id:%d err:%v", id, err))
        }

        if l.Id != id {
            panic(l)
        }

        id++
    }
}