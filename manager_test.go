package dm

import (
    "fmt"
    "testing"
    "time"

    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/utils"
)

func TestDataManager_SlowQueryLog(t *testing.T) {
    m := NewDataManager(&testRulerByFieldInvertFilterMap{})
    m.SetSlowQueryLog(func(noinvert FieldMap, cost time.Duration) {
        fmt.Printf("noinvert:%+v cost:%+v\n", noinvert, cost)
    })

    cost := utils.CostRun(func() {
        for i := 0; i < 2000000; i++ {
            a := A{
                Idx:  i + 1,
                SId:  i % 1000,
                Name: fmt.Sprintf("A%d", i),
            }
            testInsert(m, &a)
        }
    })
    fmt.Printf("insert cost:%+v\n", cost)

    var queryCost time.Duration
    var queryCount int

    ax := A{}
    if err := m.QueryByFields(FieldMap{"Name": "A20"}).FetchCost(&queryCost).FetchCount(&queryCount).FetchOne(&ax).GetError(); err != nil {
        t.Fatal(err)
    }
    fmt.Printf("cost:%+v count:%d %+v\n", queryCost, queryCount, ax)

    ay := A{}
    if err := m.QueryByFields(FieldMap{"SId": 20}).FetchCost(&queryCost).FetchCount(&queryCount).FetchOne(&ay).GetError(); err != nil {
        t.Fatal(err)
    }
    fmt.Printf("cost:%+v count:%d %+v\n", queryCost, queryCount, ay)

    detail := QueryCostDetail{}
    az := A{}
    if err := m.QueryByFields(FieldMap{"Name": "A20"}).FetchCostDetail(&detail).FetchCount(&queryCount).FetchOne(&az).GetError(); err != nil {
        t.Fatal(err)
    }
    fmt.Printf("cost detail:%+v count:%d %+v\n", detail, queryCount, az)
}