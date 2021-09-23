package dm

import (
    "fmt"
    "sort"
    "time"

    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/convert"
    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/runtime"
    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/utils"
    "github.com/pkg/errors"
)

// BaseQueryIdsResult IQueryResult
type BaseQueryIdsResult struct {
    dataManager *DataManager    // 数据管理器
    dataIds     []DataIndex     // 数据结果集
    dataCompare Comparer        // 数据比较函数
    queryCost   time.Duration   // 查询耗时
    missIndex   FieldMap        // 查询未命中字段
    lastErr     error           // 执行过程中的错误
}

func NewBaseQueryIdsResult(manager *DataManager, id ...DataIndex) *BaseQueryIdsResult {
    return &BaseQueryIdsResult{
        dataManager: manager,
        dataIds: id,
    }
}

/************* Sort接口实现 *************/

func (bq *BaseQueryIdsResult) Len() int {
    return len(bq.dataIds)
}

func (bq *BaseQueryIdsResult) Swap(i, j int) {
    bq.dataIds[i], bq.dataIds[j] = bq.dataIds[j], bq.dataIds[i]
}

func (bq *BaseQueryIdsResult) Less(i, j int) bool {
    if bq.dataCompare != nil {
        iRow, has := bq.dataManager.getDataByIndexUnsafe(bq.dataIds[i])
        if !has {
            return false
        }
        jRow, has := bq.dataManager.getDataByIndexUnsafe(bq.dataIds[j])
        if !has {
            return false
        }
        return bq.dataCompare(iRow, jRow)
    }
    return false
}

/************* 数据操作类 *************/

// Sift 数据过滤
func (bq *BaseQueryIdsResult) Sift(f Filter) (next IQueryResult) {
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
        exchange := make([]DataIndex, 0, bq.GetSize())
        for i := 0; i < size; i++ {
            row, has := bq.dataManager.getDataByIndexUnsafe(bq.dataIds[i])
            if !has {
                continue
            }

            exclude, err := f(row)
            if err != nil {
                bq.lastErr = errors.Wrap(err, runtime.CallerFunction())
                return
            }

            // 数据不被过滤 -> 加入到exchange
            if !exclude {
                exchange = append(exchange, bq.dataIds[i])
            }
        }
        bq.dataIds = exchange
    }
    return
}

// Sort 数据排序
func (bq *BaseQueryIdsResult) Sort(c Comparer) (next IQueryResult) {
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
func (bq *BaseQueryIdsResult) Slice(s, e int) (next IQueryResult) {
    next = bq
    if bq.lastErr != nil {
        return
    }

    // 切片检测
    s, e, _ = bq.checkRegion(s, e, len(bq.dataIds))

    // 不涉及到指针内容读取 -> 不需要加锁
    bq.dataIds = bq.dataIds[s:e]
    return
}

// Page 数据分页
func (bq *BaseQueryIdsResult) Page(cur, per int) (next IQueryResult) {
    start, end := utils.PageToSection(cur, per)
    return bq.Slice(start, end)
}

// Reverse 数据反序
func (bq *BaseQueryIdsResult) Reverse() (next IQueryResult) {
    next = bq
    if bq.lastErr != nil {
        return
    }

    // 不涉及到指针内容读取 -> 不需要加锁
    if bq.dataIds == nil {
        return
    }

    length := len(bq.dataIds)
    for from, to := 0, length-1; from < to; from, to = from+1, to-1 {
        bq.dataIds[from], bq.dataIds[to] = bq.dataIds[to], bq.dataIds[from]
    }

    return
}

/************* 数据获取类 *************/

func (bq *BaseQueryIdsResult) FetchCost(cost *time.Duration) (next IQueryResult) {
    next = bq
    if cost != nil {
        *cost = bq.queryCost
    }
    return
}

func (bq *BaseQueryIdsResult) FetchCostDetail(detail *QueryCostDetail) (next IQueryResult) {
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

func (bq *BaseQueryIdsResult) FetchCount(count *int) (next IQueryResult) {
    next = bq
    if count != nil {
        *count = bq.GetSize()
    }
    return
}

func (bq *BaseQueryIdsResult) FetchOne(bean AnyData) (next IQueryResult) {
    next = bq
    if bq.lastErr != nil {
        return
    }

    // 涉及到指针内容读取 -> 需要加锁
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

func (bq *BaseQueryIdsResult) FetchSome(beans AnyDatas, s, e int) (next IQueryResult) {
    next = bq
    if bq.lastErr != nil {
        return
    }

    if bq.GetSize() == 0 {
        bq.lastErr = ErrNoData
        return
    }

    s, e, bq.lastErr = bq.checkRegion(s, e, len(bq.dataIds))
    if bq.lastErr != nil {
        return
    }

    // 涉及到指针内容读取 -> 需要加锁
    bq.dataManager.RLock()
    defer bq.dataManager.RUnlock()

    // 获取数据切片
    someData := bq.dataIds[s:e]
    if size := len(someData); size > 0 {
        data := make([]AnyData, 0, size)
        for i := 0; i < size; i++ {
            row, has := bq.dataManager.getDataByIndex(bq.dataIds[i])
            if !has {
                continue
            }
            data = append(data, row)
        }

        // 数据拷贝
        if err := utils.Clone(data, beans); err != nil {
            bq.lastErr = errors.Wrapf(err, "%s Clone", runtime.CallerFunction())
        }
    }
    return
}

func (bq *BaseQueryIdsResult) FetchAll(beans AnyDatas) (next IQueryResult) {
    next = bq
    if bq.lastErr != nil {
        return
    }

    if bq.GetSize() == 0 {
        bq.lastErr = ErrNoData
        return
    }

    // 涉及到指针内容读取 -> 需要加锁
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

func (bq *BaseQueryIdsResult) FetchMap(beansMap AnyData) (next IQueryResult) {
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

    // 涉及到指针内容读取 -> 需要加锁
    bq.dataManager.RLock()
    defer bq.dataManager.RUnlock()

    if size := bq.GetSize(); size > 0 {
        dataMap := make(map[DataIndex]AnyData)
        for i := 0; i < size; i++ {
            row, has := bq.dataManager.getDataByIndex(bq.dataIds[i])
            if !has {
                continue
            }
            dataMap[bq.dataIds[i]] = row
        }

        if err := utils.Clone(dataMap, beansMap); err != nil {
            bq.lastErr = errors.Wrapf(err, "%s Clone", runtime.CallerFunction())
        }
    }
    return
}

func (bq *BaseQueryIdsResult) FetchIndexs(indexs AnyDatas) (next IQueryResult) {
    next = bq
    if bq.lastErr != nil {
        return
    }

    defer func() {
        if err := recover(); err != nil {
            bq.lastErr = errors.Wrap(convert.Error(err), runtime.CallerFunction())
        }
    }()

    if bq.GetSize() <= 0 {
        bq.lastErr = ErrNoData
        return
    }

    if err := utils.Clone(bq.dataIds, indexs); err != nil {
        bq.lastErr = errors.Wrapf(err, "%s Clone", runtime.CallerFunction())
    }

    return bq
}

/************* 信息获取类 *************/

func (bq *BaseQueryIdsResult) GetSize() int {
    return len(bq.dataIds)
}

func (bq *BaseQueryIdsResult) GetError() error {
    // 没有错误 && 没有数据 -> 设置ErrNoData
    if bq.lastErr == nil && bq.GetSize() == 0 {
        bq.lastErr = ErrNoData
    }
    return bq.lastErr
}

func (bq *BaseQueryIdsResult) GetOne() (AnyData, error) {
    if bq.lastErr != nil {
        return nil, bq.lastErr
    }

    // 涉及到指针内容读取 -> 需要加锁
    bq.dataManager.RLock()
    defer bq.dataManager.RUnlock()

    return bq.getOne() // todo 待修改
}

func (bq *BaseQueryIdsResult) GetAll() ([]AnyData, error) {
    if bq.lastErr != nil {
        return nil, bq.lastErr
    }

    // 涉及到指针内容读取 -> 需要加锁
    bq.dataManager.RLock()
    defer bq.dataManager.RUnlock()

    return bq.getAll() // todo 待修改
}

func (bq *BaseQueryIdsResult) SetError(err error) {
    if bq.lastErr != nil {
        bq.lastErr = errors.Wrapf(err, "%+v", err)
    } else {
        bq.lastErr = err
    }
}

func (bq *BaseQueryIdsResult) SetResult(result IQueryResult) {

}

func (bq *BaseQueryIdsResult) PushIds(ids []DataIndex) {
    bq.dataIds = ids
}

func (bq *BaseQueryIdsResult) getOne() (AnyData, error) {
    if bq.GetSize() == 0 {
        return nil, ErrNoData
    }
    data, has := bq.dataManager.getDataByIndex(bq.dataIds[0])
    if !has {
        return nil, ErrNoData
    }
    return data, nil
}

func (bq *BaseQueryIdsResult) getAll() ([]AnyData, error) {
    if size := len(bq.dataIds); size > 0 {
        data := make([]AnyData, 0, size)
        for i := 0; i < size; i++ {
            row, has := bq.dataManager.getDataByIndex(bq.dataIds[i])
            if !has {
                // 如果数据不存在->读取下一个
                continue
            }
            data = append(data, row)
        }
        return data, nil
    }
    return nil, nil
}

func (bq *BaseQueryIdsResult) makeDataResult(size int) {
    bq.dataIds = make([]DataIndex, 0, size)
}

func (bq *BaseQueryIdsResult) pushId(index DataIndex) {
    bq.dataIds = append(bq.dataIds, index)
}

func (bq *BaseQueryIdsResult) checkRegion(start, end, size int) (safeStart, safeEnd int, err error) {
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
