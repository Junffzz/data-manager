package dm

import (
    "fmt"
    "runtime"
    "testing"

    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/container/dm/croaring"
)

func ExampleNewIdsManager() {
    ids := NewIdsManager()
    ids.Add(3)
    ids.Add(4)
    ids.Add(5)
    ids.Add(11)
    ids.Add(3)
    ids.Add(5)

    fmt.Printf("%+v\n", ids.ToArray())
    fmt.Printf("%+v\n", ids.Contains(44))
    fmt.Printf("%+v\n", ids.Contains(5))

    // Output:
    // [3 4 5 11]
    // false
    // true
}

func TestInvertScatterMemory_Bitmap(t *testing.T) {
    for i := 0; i < 10000000; i++ {
        croaring.New(10)
    }

    // C侧内存 -> 捕获不到
    mem := runtime.MemStats{}
    runtime.ReadMemStats(&mem)

    fmt.Printf("mem:%+v\n", mem)
}

func TestInvertScatterMemory_Uint32(t *testing.T) {
    n := make([]uint32, 10000000)
    for i := 0; i < 10000000; i++ {
        n[i] = uint32(i)
    }
    fmt.Printf("%+v\n", len(n))

    mem := runtime.MemStats{}
    runtime.ReadMemStats(&mem)

    fmt.Printf("mem:%+v\n", mem)
}

func TestInvertScatterMemory_Uint64(t *testing.T) {
    n := make([]uint64, 10000000)
    for i := 0; i < 10000000; i++ {
        n[i] = uint64(i)
    }

    fmt.Printf("l:%+v\n", len(n))

    mem := runtime.MemStats{}
    runtime.ReadMemStats(&mem)

    fmt.Printf("mem:%+v\n", mem)
}