package dm

import (
    "fmt"
    "sync"
    "testing"
    "time"
    "unsafe"

    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/convert"
    memory_tile "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/encoding/memory-tile"
)

type A struct {
    Idx  int   `dm:"index"`
    SId  int   `dm:"invert"`
    XIds []int `dm:"invert"`
    Name string
}

func testInsert(manager *DataManager, data AnyData, debug ...bool) {
    if err := manager.Insert(data); err != nil {
        panic(err)
    } else {
        if len(debug) > 0 && debug[0] {
            fmt.Printf("Insert %+v success\n", data)
        }
    }
}

func testDelete(manager *DataManager, index DataIndex) {
    if err := manager.Delete(index); err != nil {
        panic(err)
    } else {
        fmt.Printf("Delete index:%v success\n", index)
    }
}

func testGetDatasByField(manager *DataManager, filedMap FieldMap) {
    results := manager.QueryByFields(filedMap)
    datas, err := results.GetAll()
    if err != nil {
        panic(err)
    } else {
        slice := convert.Slice(datas)
        if len(slice) == 0 {
            fmt.Println("[]")
            return
        }

        for idx, data := range slice {
            switch value := data.(type) {
            case *A:
                fmt.Printf("A:%+v", value)
            default:
                fmt.Printf("idx:%d data type error", idx)
            }

            if idx != len(slice)-1 {
                fmt.Print(" ")
            }
        }
        fmt.Print("\n")
    }

}

func testGetIdsByField(manager *DataManager, filedMap FieldMap) {
    results := manager.QueryIdsByFields(filedMap)
    datas, err := results.GetAll()
    if err != nil {
        panic(err)
    } else {
        slice := convert.Slice(datas)
        if len(slice) == 0 {
            fmt.Println("[]")
            return
        }

        for idx, data := range slice {
            switch value := data.(type) {
            case *A:
                fmt.Printf("A:%+v", value)
            default:
                fmt.Printf("idx:%d data type error", idx)
            }

            if idx != len(slice)-1 {
                fmt.Print(" ")
            }
        }
        fmt.Print("\n")
    }
}

func testModifyDataByIndex(manager *DataManager, index int, modifier func(dataPtr AnyData) error) {
    err := manager.ModifyDataByIndex(modifier, index)
    if err != nil {
        panic(err)
    } else {
        fmt.Printf("Modify success. index:%d\n", index)
    }
}

// 测试QueryResult的链式调用
func ExampleDataManager_ResultLinkCall() {
    m := NewDataManager()

    w := sync.WaitGroup{}
    w.Add(1)

    go func() {
        for i := 0; i < 30; i++ {
            a1 := A{
                Idx:  i,
                SId:  i % 2,
                XIds: []int{i % 3},
                Name: "A1",
            }
            testInsert(m, &a1, true)
        }
        w.Done()
    }()
    w.Wait()

    var slice []A
    var maps map[int]A
    var indexs []int

    // SId:1 XIds:[2]
    err := m.QueryByFields(FieldMap{"SId": 1, "XIds": 2}).Sort(func(iObj, jObj interface{}) bool {
        i := iObj.(*A)
        j := jObj.(*A)
        return i.Idx > j.Idx
    }).Slice(0, 3).FetchMap(&maps).FetchIndexs(&indexs).FetchAll(&slice).GetError()

    if err != nil {
        panic(err)
    }

    fmt.Printf("%+v\n", maps)
    fmt.Printf("%+v\n", indexs)
    fmt.Printf("%+v\n", slice)

    // SId:1 XIds:[2]
    err = m.QueryIdsByFields(FieldMap{"SId": 1, "XIds": 2}).Sort(func(iObj, jObj interface{}) bool {
        i := (*A)(iObj.(unsafe.Pointer))
        j := (*A)(jObj.(unsafe.Pointer))
        return i.Idx > j.Idx
    }).Slice(0, 3).FetchMap(&maps).FetchIndexs(&indexs).FetchAll(&slice).GetError()

    if err != nil {
        panic(err)
    }

    fmt.Printf("%+v\n", maps)
    fmt.Printf("%+v\n", indexs)
    fmt.Printf("%+v\n", slice)

    // Output:
    // Insert &{Idx:0 SId:0 XIds:[0] Name:A1} success
    // Insert &{Idx:1 SId:1 XIds:[1] Name:A1} success
    // Insert &{Idx:2 SId:0 XIds:[2] Name:A1} success
    // Insert &{Idx:3 SId:1 XIds:[0] Name:A1} success
    // Insert &{Idx:4 SId:0 XIds:[1] Name:A1} success
    // Insert &{Idx:5 SId:1 XIds:[2] Name:A1} success
    // Insert &{Idx:6 SId:0 XIds:[0] Name:A1} success
    // Insert &{Idx:7 SId:1 XIds:[1] Name:A1} success
    // Insert &{Idx:8 SId:0 XIds:[2] Name:A1} success
    // Insert &{Idx:9 SId:1 XIds:[0] Name:A1} success
    // Insert &{Idx:10 SId:0 XIds:[1] Name:A1} success
    // Insert &{Idx:11 SId:1 XIds:[2] Name:A1} success
    // Insert &{Idx:12 SId:0 XIds:[0] Name:A1} success
    // Insert &{Idx:13 SId:1 XIds:[1] Name:A1} success
    // Insert &{Idx:14 SId:0 XIds:[2] Name:A1} success
    // Insert &{Idx:15 SId:1 XIds:[0] Name:A1} success
    // Insert &{Idx:16 SId:0 XIds:[1] Name:A1} success
    // Insert &{Idx:17 SId:1 XIds:[2] Name:A1} success
    // Insert &{Idx:18 SId:0 XIds:[0] Name:A1} success
    // Insert &{Idx:19 SId:1 XIds:[1] Name:A1} success
    // Insert &{Idx:20 SId:0 XIds:[2] Name:A1} success
    // Insert &{Idx:21 SId:1 XIds:[0] Name:A1} success
    // Insert &{Idx:22 SId:0 XIds:[1] Name:A1} success
    // Insert &{Idx:23 SId:1 XIds:[2] Name:A1} success
    // Insert &{Idx:24 SId:0 XIds:[0] Name:A1} success
    // Insert &{Idx:25 SId:1 XIds:[1] Name:A1} success
    // Insert &{Idx:26 SId:0 XIds:[2] Name:A1} success
    // Insert &{Idx:27 SId:1 XIds:[0] Name:A1} success
    // Insert &{Idx:28 SId:0 XIds:[1] Name:A1} success
    // Insert &{Idx:29 SId:1 XIds:[2] Name:A1} success
    // map[17:{Idx:17 SId:1 XIds:[2] Name:A1} 23:{Idx:23 SId:1 XIds:[2] Name:A1} 29:{Idx:29 SId:1 XIds:[2] Name:A1}]
    // [29 23 17]
    // [{Idx:29 SId:1 XIds:[2] Name:A1} {Idx:23 SId:1 XIds:[2] Name:A1} {Idx:17 SId:1 XIds:[2] Name:A1}]
    // map[17:{Idx:17 SId:1 XIds:[2] Name:A1} 23:{Idx:23 SId:1 XIds:[2] Name:A1} 29:{Idx:29 SId:1 XIds:[2] Name:A1}]
    // [29 23 17]
    // [{Idx:29 SId:1 XIds:[2] Name:A1} {Idx:23 SId:1 XIds:[2] Name:A1} {Idx:17 SId:1 XIds:[2] Name:A1}]
}

// 综合测试倒排更新和查询的过程
func ExampleDataManager_QueryByFields() {
    m := NewDataManager()

    a1 := A{
        Idx:  2,
        SId:  5,
        XIds: []int{3, 4, 5, 6, 7},
        Name: "A1",
    }
    testInsert(m, &a1, true)

    a2 := A{
        Idx:  3,
        SId:  5,
        XIds: []int{3, 11, 12, 13, 14, 6},
        Name: "A2",
    }
    testInsert(m, &a2, true)

    testGetDatasByField(m, FieldMap{"SId": 5})
    testGetIdsByField(m, FieldMap{"SId": 5})
    testGetDatasByField(m, FieldMap{"SId": 5, "XIds": 11})
    testGetIdsByField(m, FieldMap{"SId": 5, "XIds": 11})

    // []
    testGetDatasByField(m, FieldMap{"SId": 5, "XIds": 20})
    testGetIdsByField(m, FieldMap{"SId": 5, "XIds": 20})

    // 3: SId -> 6
    ptr, err := m.UpdateAndReturn(3, FieldInfosToFieldMap(FieldInfo{"SId", 6}))
    if err != nil {
        panic(err)
    }
    fmt.Printf("%+v\n", convert.Direct(ptr))

    testGetDatasByField(m, FieldMap{"SId": 5})
    testGetIdsByField(m, FieldMap{"SId": 5})
    testGetDatasByField(m, FieldMap{"SId": 6})
    testGetIdsByField(m, FieldMap{"SId": 6})

    // 3: SId -> 7
    testModifyDataByIndex(m, 3, func(dataPtr AnyData) error {
        if value, ok := dataPtr.(*A); ok {
            value.SId = 7
        }
        return nil
    })

    testGetDatasByField(m, FieldMap{"SId": 5})
    testGetIdsByField(m, FieldMap{"SId": 5})
    testGetDatasByField(m, FieldMap{"SId": 7})
    testGetIdsByField(m, FieldMap{"SId": 7})
    testGetDatasByField(m, FieldMap{"XIds": 3})
    testGetIdsByField(m, FieldMap{"XIds": 3})
    testGetDatasByField(m, FieldMap{"XIds": 11})
    testGetIdsByField(m, FieldMap{"XIds": 11})

    // 3: XIds -> [3, 12, 13, 14, 6]
    prepare := func(dataPtr AnyData) FieldNames {
        return FieldNames{"XIds"}
    }
    modify := func(dataPtr AnyData) error {
        a := dataPtr.(*A)
        a.XIds = []int{3, 12, 13, 14, 6}
        return nil
    }
    if err := m.ModifyDataByIndexEx(prepare, modify, 3); err != nil {
        panic(err)
    }

    testGetDatasByField(m, FieldMap{"XIds": 3})
    testGetIdsByField(m, FieldMap{"XIds": 3})
    testGetDatasByField(m, FieldMap{"XIds": 11})
    testGetIdsByField(m, FieldMap{"XIds": 11})
    testGetDatasByField(m, FieldMap{"XIds": 12})
    testGetIdsByField(m, FieldMap{"XIds": 12})

    testDelete(m, 2)
    testGetDatasByField(m, FieldMap{"SId": 5})
    testGetIdsByField(m, FieldMap{"SId": 5})
    testGetDatasByField(m, FieldMap{"SId": 7})
    testGetIdsByField(m, FieldMap{"SId": 7})
    testGetDatasByField(m, FieldMap{"XIds": 3})
    testGetIdsByField(m, FieldMap{"XIds": 3})
    testGetDatasByField(m, FieldMap{"XIds": 12})
    testGetIdsByField(m, FieldMap{"XIds": 12})

    // Output:
    // Insert &{Idx:2 SId:5 XIds:[3 4 5 6 7] Name:A1} success
    // Insert &{Idx:3 SId:5 XIds:[3 11 12 13 14 6] Name:A2} success
    // A:&{Idx:2 SId:5 XIds:[3 4 5 6 7] Name:A1} A:&{Idx:3 SId:5 XIds:[3 11 12 13 14 6] Name:A2}
    // A:&{Idx:2 SId:5 XIds:[3 4 5 6 7] Name:A1} A:&{Idx:3 SId:5 XIds:[3 11 12 13 14 6] Name:A2}
    // A:&{Idx:3 SId:5 XIds:[3 11 12 13 14 6] Name:A2}
    // A:&{Idx:3 SId:5 XIds:[3 11 12 13 14 6] Name:A2}
    // []
    // []
    // {Idx:3 SId:6 XIds:[3 11 12 13 14 6] Name:A2}
    // A:&{Idx:2 SId:5 XIds:[3 4 5 6 7] Name:A1}
    // A:&{Idx:2 SId:5 XIds:[3 4 5 6 7] Name:A1}
    // A:&{Idx:3 SId:6 XIds:[3 11 12 13 14 6] Name:A2}
    // A:&{Idx:3 SId:6 XIds:[3 11 12 13 14 6] Name:A2}
    // Modify success. index:3
    // A:&{Idx:2 SId:5 XIds:[3 4 5 6 7] Name:A1}
    // A:&{Idx:2 SId:5 XIds:[3 4 5 6 7] Name:A1}
    // A:&{Idx:3 SId:7 XIds:[3 11 12 13 14 6] Name:A2}
    // A:&{Idx:3 SId:7 XIds:[3 11 12 13 14 6] Name:A2}
    // A:&{Idx:2 SId:5 XIds:[3 4 5 6 7] Name:A1} A:&{Idx:3 SId:7 XIds:[3 11 12 13 14 6] Name:A2}
    // A:&{Idx:2 SId:5 XIds:[3 4 5 6 7] Name:A1} A:&{Idx:3 SId:7 XIds:[3 11 12 13 14 6] Name:A2}
    // A:&{Idx:3 SId:7 XIds:[3 11 12 13 14 6] Name:A2}
    // A:&{Idx:3 SId:7 XIds:[3 11 12 13 14 6] Name:A2}
    // A:&{Idx:2 SId:5 XIds:[3 4 5 6 7] Name:A1} A:&{Idx:3 SId:7 XIds:[3 12 13 14 6] Name:A2}
    // A:&{Idx:2 SId:5 XIds:[3 4 5 6 7] Name:A1} A:&{Idx:3 SId:7 XIds:[3 12 13 14 6] Name:A2}
    // []
    // []
    // A:&{Idx:3 SId:7 XIds:[3 12 13 14 6] Name:A2}
    // A:&{Idx:3 SId:7 XIds:[3 12 13 14 6] Name:A2}
    // Delete index:2 success
    // []
    // []
    // A:&{Idx:3 SId:7 XIds:[3 12 13 14 6] Name:A2}
    // A:&{Idx:3 SId:7 XIds:[3 12 13 14 6] Name:A2}
    // A:&{Idx:3 SId:7 XIds:[3 12 13 14 6] Name:A2}
    // A:&{Idx:3 SId:7 XIds:[3 12 13 14 6] Name:A2}
    // A:&{Idx:3 SId:7 XIds:[3 12 13 14 6] Name:A2}
    // A:&{Idx:3 SId:7 XIds:[3 12 13 14 6] Name:A2}
}

// 综合测试倒排更新和查询的过程
func ExampleDataManager_QueryMultiByFields() {
    m := NewDataManager()

    a1 := A{
        Idx:  2,
        SId:  5,
        XIds: []int{4, 5, 7},
        Name: "A1",
    }
    testInsert(m, &a1, true)

    a2 := A{
        Idx:  3,
        SId:  4,
        XIds: []int{3, 11, 12, 13, 14, 6},
        Name: "A2",
    }
    testInsert(m, &a2, true)

    a3 := A{
        Idx:  4,
        SId:  3,
        XIds: []int{3, 6},
        Name: "A3",
    }
    testInsert(m, &a3, true)

    // (a1 ∪ a2 ∪ a3) ∩ (a1 ∪ a2 ∪ a3) = a1 ∪ a2 ∪ a3
    testGetDatasByField(m, FieldMap{"SId": []int{3, 4, 5}, "XIds": []int{3, 4, 6}})
    testGetIdsByField(m, FieldMap{"SId": []int{3, 4, 5}, "XIds": []int{3, 4, 6}})

    // (a1 ∪ a2) ∩ (a1 ∪ a2 ∪ a3) = a1 ∪ a2
    testGetDatasByField(m, FieldMap{"SId": []int{4, 5}, "XIds": []int{3, 4, 6}})
    testGetIdsByField(m, FieldMap{"SId": []int{4, 5}, "XIds": []int{3, 4, 6}})

    // (a1 ∪ a2 ∪ a3) ∩ (a2 ∪ a3) = a2 ∪ a3
    testGetDatasByField(m, FieldMap{"SId": []int{3, 4, 5}, "XIds": []int{3, 6}})
    testGetIdsByField(m, FieldMap{"SId": []int{3, 4, 5}, "XIds": []int{3, 6}})

    // (a1 ∪ a2) ∩ (a2 ∪ a3) = a2
    testGetDatasByField(m, FieldMap{"SId": []int{4, 5}, "XIds": []int{3, 6}})
    testGetIdsByField(m, FieldMap{"SId": []int{4, 5}, "XIds": []int{3, 6}})

    // (a1 ∪ a2 ∪ a3) ∩ (a1 ∪ a2 ∪ a3) ∩ a2 = a2
    testGetDatasByField(m, FieldMap{"SId": []int{3, 4, 5}, "XIds": []int{3, 4, 6}, "Idx": 3})
    testGetIdsByField(m, FieldMap{"SId": []int{3, 4, 5}, "XIds": []int{3, 4, 6}, "Idx": 3})

    // (a1 ∪ a2 ∪ a3) ∩ (a1 ∪ a2 ∪ a3) ∩ (a2 ∪ a3) = a2 ∪ a3
    testGetDatasByField(m, FieldMap{"SId": []int{3, 4, 5}, "XIds": []int{3, 4, 6}, "Idx": []int{3, 4}})
    testGetIdsByField(m, FieldMap{"SId": []int{3, 4, 5}, "XIds": []int{3, 4, 6}, "Idx": []int{3, 4}})

    // (a1 ∪ a2 ∪ a3) ∩ (a1 ∪ a2 ∪ a3) ∩ a2 = a2
    testGetDatasByField(m, FieldMap{"SId": []int{3, 4, 5}, "XIds": []int{3, 4, 6}}.AddDataIndex(3))
    testGetIdsByField(m, FieldMap{"SId": []int{3, 4, 5}, "XIds": []int{3, 4, 6}}.AddDataIndex(3))

    // Output:
    // Insert &{Idx:2 SId:5 XIds:[4 5 7] Name:A1} success
    // Insert &{Idx:3 SId:4 XIds:[3 11 12 13 14 6] Name:A2} success
    // Insert &{Idx:4 SId:3 XIds:[3 6] Name:A3} success
    // A:&{Idx:2 SId:5 XIds:[4 5 7] Name:A1} A:&{Idx:3 SId:4 XIds:[3 11 12 13 14 6] Name:A2} A:&{Idx:4 SId:3 XIds:[3 6] Name:A3}
    // A:&{Idx:2 SId:5 XIds:[4 5 7] Name:A1} A:&{Idx:3 SId:4 XIds:[3 11 12 13 14 6] Name:A2} A:&{Idx:4 SId:3 XIds:[3 6] Name:A3}
    // A:&{Idx:2 SId:5 XIds:[4 5 7] Name:A1} A:&{Idx:3 SId:4 XIds:[3 11 12 13 14 6] Name:A2}
    // A:&{Idx:2 SId:5 XIds:[4 5 7] Name:A1} A:&{Idx:3 SId:4 XIds:[3 11 12 13 14 6] Name:A2}
    // A:&{Idx:3 SId:4 XIds:[3 11 12 13 14 6] Name:A2} A:&{Idx:4 SId:3 XIds:[3 6] Name:A3}
    // A:&{Idx:3 SId:4 XIds:[3 11 12 13 14 6] Name:A2} A:&{Idx:4 SId:3 XIds:[3 6] Name:A3}
    // A:&{Idx:3 SId:4 XIds:[3 11 12 13 14 6] Name:A2}
    // A:&{Idx:3 SId:4 XIds:[3 11 12 13 14 6] Name:A2}
    // A:&{Idx:3 SId:4 XIds:[3 11 12 13 14 6] Name:A2}
    // A:&{Idx:3 SId:4 XIds:[3 11 12 13 14 6] Name:A2}
    // A:&{Idx:3 SId:4 XIds:[3 11 12 13 14 6] Name:A2} A:&{Idx:4 SId:3 XIds:[3 6] Name:A3}
    // A:&{Idx:3 SId:4 XIds:[3 11 12 13 14 6] Name:A2} A:&{Idx:4 SId:3 XIds:[3 6] Name:A3}
    // A:&{Idx:3 SId:4 XIds:[3 11 12 13 14 6] Name:A2}
    // A:&{Idx:3 SId:4 XIds:[3 11 12 13 14 6] Name:A2}
}

/********************************************************/

type testRulerByFieldInvertFilter struct {
}

func (t *testRulerByFieldInvertFilter) FieldInvertFilter(data AnyData, fieldName string, fieldValue AnyData) (build bool) {
    fmt.Printf("name:%+v value:%+v\n", fieldName, fieldValue)

    a := data.(*A)
    if a.Name == "A1" {
        return false
    }
    return true
}

func (t *testRulerByFieldInvertFilter) FieldInvertFilterMap(data AnyData) (filter FieldMap) {
    return nil
}

func ExampleDataManager_FieldInvertFilter() {
    m := NewDataManager(&testRulerByFieldInvertFilter{})

    a1 := A{
        Idx:  2,
        SId:  5,
        Name: "A1",
    }
    testInsert(m, &a1, true)

    a2 := A{
        Idx:  3,
        SId:  5,
        Name: "A2",
    }
    testInsert(m, &a2, true)

    // A:&{Idx:3 SId:5 XIds:[] Name:A2}
    testGetDatasByField(m, FieldMap{"SId": 5})
    testGetIdsByField(m, FieldMap{"SId": 5})

    // 2: Name -> A3, SId -> 4 符合构建倒排条件
    if err := m.Update(2, FieldMap{"SId": 4, "Name": "A3"}); err != nil {
        panic(err)
    }
    testGetDatasByField(m, FieldMap{"SId": 4})
    testGetIdsByField(m, FieldMap{"SId": 4})

    testGetDatasByField(m, FieldMap{"SId": 5})
    testGetIdsByField(m, FieldMap{"SId": 5})

    // 更新Name -> 重新构建倒排索引
    if err := m.Update(2, FieldMap{"Name": "A1"}); err != nil {
        panic(err)
    }
    if err := m.RebuildInvert(2); err != nil {
        panic(err)
    }
    testGetDatasByField(m, FieldMap{"SId": 4})
    testGetIdsByField(m, FieldMap{"SId": 4})

    testGetDatasByField(m, FieldMap{"SId": 5})
    testGetIdsByField(m, FieldMap{"SId": 5})

    // Output:
    // name:SId value:5
    // Insert &{Idx:2 SId:5 XIds:[] Name:A1} success
    // name:SId value:5
    // Insert &{Idx:3 SId:5 XIds:[] Name:A2} success
    // A:&{Idx:3 SId:5 XIds:[] Name:A2}
    // A:&{Idx:3 SId:5 XIds:[] Name:A2}
    // name:SId value:4
    // A:&{Idx:2 SId:4 XIds:[] Name:A3}
    // A:&{Idx:2 SId:4 XIds:[] Name:A3}
    // A:&{Idx:3 SId:5 XIds:[] Name:A2}
    // A:&{Idx:3 SId:5 XIds:[] Name:A2}
    // name:SId value:4
    // name:SId value:4
    // []
    // []
    // A:&{Idx:3 SId:5 XIds:[] Name:A2}
    // A:&{Idx:3 SId:5 XIds:[] Name:A2}
}

/********************************************************/

type testRulerByFieldInvertFilterMap struct {
}

func (t *testRulerByFieldInvertFilterMap) FieldInvertFilter(data AnyData, fieldName string, fieldValue AnyData) (build bool) {
    return true
}

func (t *testRulerByFieldInvertFilterMap) FieldInvertFilterMap(data AnyData) (filter FieldMap) {
    a := data.(*A)
    if a.Name == "A1" {
        // 空filter -> 全部过滤
        filter = make(FieldMap)
        return filter
    }

    // 不进行过滤
    return nil
}

// 倒排索引构建定制过滤, 基于FieldInvertFilterMap
func ExampleDataManager_FieldInvertFilterMap() {
    m := NewDataManager(&testRulerByFieldInvertFilterMap{})

    a1 := A{
        Idx:  2,
        SId:  5,
        Name: "A1",
    }
    testInsert(m, &a1, true)

    a2 := A{
        Idx:  3,
        SId:  5,
        Name: "A2",
    }
    testInsert(m, &a2, true)

    // A:&{Idx:3 SId:5 XIds:[] Name:A2}
    testGetDatasByField(m, FieldMap{"SId": 5})
    testGetIdsByField(m, FieldMap{"SId": 5})

    // 3: SId -> 4, Name: A3
    if err := m.Update(3, FieldMap{"SId": 4, "Name": "A3"}); err != nil {
        panic(err)
    }
    // A:&{Idx:3 SId:4 XIds:[] Name:A3}
    testGetDatasByField(m, FieldMap{"SId": 4})
    testGetIdsByField(m, FieldMap{"SId": 4})

    testGetDatasByField(m, FieldMap{"SId": 5})
    testGetIdsByField(m, FieldMap{"SId": 5})

    // 更新Name -> 重新构建倒排索引 (因为Update只更新指定字段的倒排, 所以需要RebuildInvert)
    // 2: Name -> A2
    if err := m.Update(2, FieldMap{"Name": "A2"}); err != nil {
        panic(err)
    }
    // 重新构建倒排索引
    if err := m.RebuildInvert(2); err != nil {
        panic(err)
    }
    testGetDatasByField(m, FieldMap{"SId": 5})
    testGetIdsByField(m, FieldMap{"SId": 5})

    // 2: Name -> A2
    if err := m.Update(2, FieldMap{"Name": "A1"}); err != nil {
        panic(err)
    }
    // 重新构建倒排索引
    if err := m.RebuildInvert(2); err != nil {
        panic(err)
    }
    testGetDatasByField(m, FieldMap{"SId": 5})
    testGetIdsByField(m, FieldMap{"SId": 5})

    // ModifyDataByIndex全量更新倒排索引, 不需要Rebuild
    // 2: Name -> A2
    err := m.ModifyDataByIndex(func(dataPtr AnyData) error {
        a := dataPtr.(*A)
        a.Name = "A2"
        return nil
    }, 2)
    if err != nil {
        panic(err)
    }
    testGetDatasByField(m, FieldMap{"SId": 5})
    testGetIdsByField(m, FieldMap{"SId": 5})

    // Output:
    // Insert &{Idx:2 SId:5 XIds:[] Name:A1} success
    // Insert &{Idx:3 SId:5 XIds:[] Name:A2} success
    // A:&{Idx:3 SId:5 XIds:[] Name:A2}
    // A:&{Idx:3 SId:5 XIds:[] Name:A2}
    // A:&{Idx:3 SId:4 XIds:[] Name:A3}
    // A:&{Idx:3 SId:4 XIds:[] Name:A3}
    // []
    // []
    // A:&{Idx:2 SId:5 XIds:[] Name:A2}
    // A:&{Idx:2 SId:5 XIds:[] Name:A2}
    // []
    // []
    // A:&{Idx:2 SId:5 XIds:[] Name:A2}
    // A:&{Idx:2 SId:5 XIds:[] Name:A2}
}

func TestDataManager_FieldInvertFilterMap(t *testing.T) {
    m := NewDataManager(&testRulerByFieldInvertFilterMap{})

    a1 := A{
        Idx:  1,
        SId:  5,
        Name: "A1",
    }
    testInsert(m, &a1, true)

    a2 := A{
        Idx:  2,
        SId:  5,
        XIds: []int{1, 2, 3},
        Name: "A2",
    }
    testInsert(m, &a2, true)

    a3 := A{
        Idx:  3,
        SId:  5,
        Name: "A3",
    }
    testInsert(m, &a3, true)

    a4 := A{
        Idx:  4,
        SId:  6,
        Name: "A4",
    }
    testInsert(m, &a4, true)

    {
        d, e := m.QueryByFields(FieldMap{"SId": uint32(5), "XIds": uint32(3)}.AddTypeConvertFlag()).Sift(func(dataPtr AnyData) (exclude bool, err error) {
            data := dataPtr.(*A)
            if data.Name == "A3" {
                return true, nil
            }
            return false, nil
        }).GetAll()
        if e != nil {
            panic(e)
        }
        for _, data := range d {
            fmt.Printf("%+v\n", *data.(*A))
        }
    }

    {
        d, e := m.QueryIdsByFields(FieldMap{"SId": 5}).Sift(func(dataPtr AnyData) (exclude bool, err error) {
            data := (*A)(dataPtr.(unsafe.Pointer))
            if data.Name == "A3" {
                return true, nil
            }
            return false, nil
        }).GetAll()
        if e != nil {
            panic(e)
        }
        for _, data := range d {
            fmt.Printf("%+v\n", *data.(*A))
        }
    }
}

func TestDataManager_TimeInvert(t *testing.T) {
    type TimeInvert struct {
        Id   int       `dm:"index"`
        Key1 time.Time `dm:"invert"`
        Key2 string
        Key3 int
    }

    t1 := time.Now()
    t2 := time.Now().Add(-time.Hour * 24)

    today := time.Date(t1.Year(), t1.Month(), t1.Day(), 0, 0, 0, 0, time.Local)
    yesterday := time.Date(t2.Year(), t2.Month(), t2.Day(), 0, 0, 0, 0, time.Local)

    // 经过memory_tile之后 时间虽然内容相同 值不相等
    today1 := time.Time{}
    today2 := today
    if mt, err := memory_tile.Marshal(today); err != nil {
        panic(err)
    } else {
        if err := memory_tile.Unmarshal(mt, &today1); err != nil {
            panic(err)
        }

        if today1 == today {
            fmt.Println("today1 == today by memory_tile", )
        }
        if today2 == today {
            fmt.Println("today2 == today by value copy", )
        }
    }

    m := NewDataManager()
    err := m.Insert(&TimeInvert{
        Id:   1,
        Key1: today,
        Key2: "Data1",
        Key3: 4,
    })
    if err != nil {
        panic(err)
    }

    err = m.Insert(&TimeInvert{
        Id:   2,
        Key1: yesterday,
        Key2: "Data2",
        Key3: 5,
    })
    if err != nil {
        panic(err)
    }
    
    d, e := m.QueryByFields(FieldMap{"Key1" : today1}).GetAll()
    if e != nil {
        panic(e)
    }
    fmt.Printf("%+v\n", d)

    err = m.Insert(&TimeInvert{
        Id:   3,
        Key1: convert.Time("2021-07-28 11:30:21"),
        Key2: "Data1",
        Key3: 4,
    })
    if err != nil {
        panic(err)
    }

    d, e = m.QueryByFields(FieldMap{"Key1" : today}).GetAll()
    if e != nil {
        panic(e)
    }
    fmt.Printf("%+v\n", d)
}
