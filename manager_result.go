package dm

import (
    "fmt"
    "sort"
    "time"

    "github.com/pkg/errors"

    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/convert"
    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/runtime"
    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/utils"
)

// 数据记录
type queryDataResult struct {
    index DataIndex   // 主键索引
    data  AnyData     // 数据
}

// BaseQueryResult IQueryResult
type BaseQueryResult struct {
    dataManager *DataManager
    dataInfos   []queryDataResult   // 数据结果集
    dataCompare Comparer            // 数据比较函数
    queryCost   time.Duration       // 查询耗时
    missIndex   FieldMap            // 查询未命中字段
    lastErr     error               // 执行过程中的错误
}

func NewBaseQueryResult(manager *DataManager, infos ...queryDataResult) *BaseQueryResult {
    return &BaseQueryResult{
        dataManager: manager,
        dataInfos: infos,
    }
}

/************* Sort接口实现 *************/

func (bq *BaseQueryResult) Len() int {
    return len(bq.dataInfos)
}

func (bq *BaseQueryResult) Swap(i, j int) {
    bq.dataInfos[i], bq.dataInfos[j] = bq.dataInfos[j], bq.dataInfos[i]
}

func (bq *BaseQueryResult) Less(i, j int) bool {
    if bq.dataCompare != nil {
        return bq.dataCompare(bq.dataInfos[i].data, bq.dataInfos[j].data)
    }
    return false
}

/************* 数据操作类 *************/

// Sift 数据过滤
func (bq *BaseQueryResult) Sift(f Filter) (next IQueryResult) {
    next = bq
    if bq.lastErr != nil || bq.GetSize() <= 0 || f == nil {
        return
    }

    defer func() {
        if err := recover(); err != nil {
            bq.lastErr = errors.Wrap(convert.Error(err), runtime.CallerFunction())
        }
    }()

    // 涉及到指针内容读取 -> 需要加锁
    bq.dataManager.RLock()
    defer bq.dataManager.RUnlock()

    // 使用f进行数据过滤
    if size := bq.GetSize(); size > 0 {
        exchange := make([]queryDataResult, 0, bq.GetSize())
        for i := 0; i < size; i++ {
            exclude, err := f(bq.dataInfos[i].data)
            if err != nil {
                bq.lastErr = errors.Wrap(err, runtime.CallerFunction())
                return
            }

            // 数据不被过滤 -> 加入到exchange
            if !exclude {
                exchange = append(exchange, bq.dataInfos[i])
            }
        }
        bq.dataInfos = exchange
    }

    return
}

// Sort 数据排序
func (bq *BaseQueryResult) Sort(c Comparer) (next IQueryResult) {
    next = bq
    if bq.lastErr != nil {
        return
    }

    // 涉及到指针内容读取 -> 需要加锁
    bq.dataManager.RLock()
    defer bq.dataManager.RUnlock()

    if c != nil {
        bq.dataCompare = c
        sort.Sort(bq)
    }
    return
}

// Slice 数据切片
func (bq *BaseQueryResult) Slice(s, e int) (next IQueryResult) {
    next = bq
    if bq.lastErr != nil {
        return
    }

    // 切片检测
    s, e, _ = bq.checkRegion(s, e, len(bq.dataInfos))

    // 不涉及到指针内容读取 -> 不需要加锁
    bq.dataInfos = bq.dataInfos[s : e]
    return
}

// Page 数据分页
func (bq *BaseQueryResult) Page(cur, per int) (next IQueryResult) {
    start, end := utils.PageToSection(cur, per)
    return bq.Slice(start, end)
}

// Reverse 数据反序
func (bq *BaseQueryResult) Reverse() (next IQueryResult) {
    next = bq
    if bq.lastErr != nil {
        return
    }

    // 不涉及到指针内容读取 -> 不需要加锁
    if bq.dataInfos == nil {
        return
    }

    length := len(bq.dataInfos)
    for from, to := 0, length - 1; from < to; from, to = from + 1, to - 1 {
        bq.dataInfos[from], bq.dataInfos[to] = bq.dataInfos[to], bq.dataInfos[from]
    }

    return
}

/************* 数据获取类 *************/

func (bq *BaseQueryResult) FetchCost(cost *time.Duration) (next IQueryResult) {
    next = bq
    if cost != nil {
        *cost = bq.queryCost
    }
    return
}

func (bq *BaseQueryResult) FetchCostDetail(detail *QueryCostDetail) (next IQueryResult) {
    next = bq
    if detail != nil {
        detail.Cost = bq.queryCost
        detail.Miss = bq.missIndex

        // 存在未命中索引的字段 -> 慢查询
        if len(detail.Miss) > 0 {
            detail.IsSlow = true
        }
    }
    return
}

func (bq *BaseQueryResult) FetchCount(count *int) (next IQueryResult) {
    next = bq
    if count != nil {
        *count = bq.GetSize()
    }
    return
}

func (bq *BaseQueryResult) FetchOne(bean AnyData) (next IQueryResult) {
    next = bq
    if bq.lastErr != nil {
        return
    }

    bq.dataManager.RLock()
    defer bq.dataManager.RUnlock()

    data, err := bq.getOne()
    if err != nil {
        bq.lastErr = errors.Wrap(err, runtime.CallerFunction())
        return
    }

    if err := utils.Clone(data, bean); err != nil {
        bq.lastErr = errors.Wrapf(err, "%s Clone", runtime.CallerFunction())
    }
    return
}

func (bq *BaseQueryResult) FetchSome(beans AnyDatas, s, e int) (next IQueryResult) {
    next = bq
    if bq.lastErr != nil {
        return
    }

    if bq.GetSize() == 0 {
        bq.lastErr = ErrNoData
        return
    }

    bq.dataManager.RLock()
    defer bq.dataManager.RUnlock()

    s, e, bq.lastErr = bq.checkRegion(s, e, len(bq.dataInfos))
    if bq.lastErr != nil {
        return
    }

    // 获取数据切片
    someData := bq.dataInfos[s : e]
    if size := len(someData); size > 0 {
        data := make([]AnyData, 0, size)
        for i := 0; i < size; i++ {
            data = append(data, someData[i].data)
        }

        // 数据拷贝
        if err := utils.Clone(data, beans); err != nil {
            bq.lastErr = errors.Wrapf(err, "%s Clone", runtime.CallerFunction())
        }
    }
    return
}

func (bq *BaseQueryResult) FetchAll(beans AnyDatas) (next IQueryResult) {
    next = bq
    if bq.lastErr != nil {
        return
    }

    if bq.GetSize() == 0 {
        bq.lastErr = ErrNoData
        return
    }

    bq.dataManager.RLock()
    defer bq.dataManager.RUnlock()

    data, err := bq.getAll()
    if err != nil {
        bq.lastErr = errors.Wrap(err, runtime.CallerFunction())
        return
    }

    if err := utils.Clone(data, beans); err != nil {
        bq.lastErr = errors.Wrapf(err, "%s Clone", runtime.CallerFunction())
    }
    return
}

func (bq *BaseQueryResult) FetchMap(beansMap AnyData) (next IQueryResult) {
    next = bq
    if bq.lastErr != nil {
        return
    }

    defer func() {
        if err := recover(); err != nil {
            bq.lastErr = errors.Wrap(convert.Error(err), runtime.CallerFunction())
        }
    }()

    if bq.GetSize() == 0 {
        bq.lastErr = ErrNoData
        return
    }

    bq.dataManager.RLock()
    defer bq.dataManager.RUnlock()

    if size := bq.GetSize(); size > 0 {
        dataMap := make(map[DataIndex]AnyData)
        for i := 0; i < size; i++ {
            dataMap[bq.dataInfos[i].index] = bq.dataInfos[i].data
        }

        if err := utils.Clone(dataMap, beansMap); err != nil {
            bq.lastErr = errors.Wrapf(err, "%s Clone", runtime.CallerFunction())
        }
    }
    return
}

func (bq *BaseQueryResult) FetchIndexs(indexs AnyDatas) (next IQueryResult) {
    next = bq
    if bq.lastErr != nil {
        return
    }

    defer func() {
        if err := recover(); err != nil {
            bq.lastErr = errors.Wrap(convert.Error(err), runtime.CallerFunction())
        }
    }()

    if bq.GetSize() == 0 {
        bq.lastErr = ErrNoData
        return
    }

    bq.dataManager.RLock()
    defer bq.dataManager.RUnlock()

    if size := bq.GetSize(); size > 0 {
        idxSlice := make([]DataIndex, 0, size)
        for i := 0; i < size; i++ {
            idxSlice = append(idxSlice, bq.dataInfos[i].index)
        }

        if err := utils.Clone(idxSlice, indexs); err != nil {
            bq.lastErr = errors.Wrapf(err, "%s Clone", runtime.CallerFunction())
        }
    }
    return bq
}

/************* 信息获取类 *************/

func (bq *BaseQueryResult) GetSize() int {
    return len(bq.dataInfos)
}

func (bq *BaseQueryResult) GetError() error {
    // 没有错误 && 没有数据 -> 设置ErrNoData
    if bq.lastErr == nil && bq.GetSize() == 0 {
        bq.lastErr = ErrNoData
    }
    return bq.lastErr
}

func (bq *BaseQueryResult) GetOne() (AnyData, error) {
    if bq.lastErr != nil {
        return nil, bq.lastErr
    }

    bq.dataManager.RLock()
    defer bq.dataManager.RUnlock()

    return bq.getOne()
}

func (bq *BaseQueryResult) GetAll() ([]AnyData, error) {
    if bq.lastErr != nil {
        return nil, bq.lastErr
    }

    bq.dataManager.RLock()
    defer bq.dataManager.RUnlock()

    return bq.getAll()
}

func (bq *BaseQueryResult) SetError(err error) {
    if bq.lastErr != nil {
        bq.lastErr = errors.Wrapf(err, "%+v", err)
    } else {
        bq.lastErr = err
    }
}

func (bq *BaseQueryResult) SetResult(result IQueryResult) {

}

func (bq *BaseQueryResult) getOne() (AnyData, error) {
    if bq.GetSize() == 0 {
        return nil, ErrNoData
    }
    return bq.dataInfos[0].data, nil
}

func (bq *BaseQueryResult) getAll() ([]AnyData, error) {
    if size := len(bq.dataInfos); size > 0 {
        data := make([]AnyData, 0, size)
        for i := 0; i < size; i++ {
            data = append(data, bq.dataInfos[i].data)
        }
        return data, nil
    }
    return nil, nil
}


func (bq *BaseQueryResult) makeDataResult(size int) {
    bq.dataInfos = make([]queryDataResult, 0, size)
}

func (bq *BaseQueryResult) pushData(index DataIndex, data AnyData) {
    bq.dataInfos = append(bq.dataInfos, queryDataResult{index: index, data: data})
}

func (bq *BaseQueryResult) checkRegion(start, end, size int) (safeStart, safeEnd int, err error) {
    if size == 0 {
        return 0, 0, fmt.Errorf("size is 0")
    }

    // -1 -> 结尾
    if end == -1 {
        end = size
    }

    // 错误区间 -> 返回空slice or string
    if start > end || start >= size || end <= 0 {
        return 0, 0, fmt.Errorf("out of region")
    }

    // 尝试恢复错误区间
    if start <= 0 {
        start = 0
    }
    if end >= size {
        end = size
    }

    return start, end, nil
}