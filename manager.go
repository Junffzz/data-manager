package dm

import (
    "errors"
    "math"
    "sync"
    "time"

    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/container/dm/croaring"
)

const TAG_NAME       = "dm"                                         // DataManager识别的Tag
const TAG_SKIP       = "-"                                          // DataManager过滤的Tag
const IDX_NAME       = "index"                                      // 主键索引的Tag
const IVT_NAME       = "invert"                                     // 倒排索引的Tag
const INVALID_IDX    = -1                                           // 无效的Id
const LIMIT_INFINITE = math.MaxInt64                                // Int的最大值
const ITERTOR_MODE   = 0                                            // 迭代模式 (0: ToArray 1: Itertor)
const INDEX_FLAG     = "*!idx!*"                                    // 主键索引的标记
const TYP_CONV_FLAG  = "*!conv!*"                                   // 倒排搜索时候类型转换标记

var KeyNotExists = errors.New("key not exist")                 // key不存在
var KeyConflict  = errors.New("key conflict")                  // key冲突
var ErrNoData    = errors.New("no data")                       // 无数据

type DataIndex   = KeyType                                          // 别名:主键索引类型
type InvertValue = interface{}                                      // 别名:倒排索引值
type AnyData     = interface{}                                      // 别名:任意数据
type AnyDatas    = interface{}                                      // 别名:任意数据(s)
type FieldName   = string                                           // 别名:字段名类型

type FieldNames     []FieldName                                     // 字段名列表
type FieldMap       map[FieldName]AnyData                           // 字段名 -> 字段值
type IndexValueMap  map[DataIndex]AnyData                           // 索引值 -> 数据
type InvertFieldMap map[FieldName]InvertValueMap                    // 索引字段名 -> (索引值 -> 数据)
type InvertValueMap map[InvertValue]croaring.Bitmap                 // 索引值 -> RBM集合

type PrepareModify  func(dataPtr AnyData) FieldNames                // 数据修改时回调, 返回准备修改什么字段
type DataModifier   func(dataPtr AnyData) error                     // 数据修改回调
type SlowQueryLog   func(noinvert FieldMap, cost time.Duration)     // 慢查询回调 (未命中索引的字段, 查询耗时)

// FieldInfo 字段信息
type FieldInfo struct {
    Name  FieldName
    Value AnyData
}

// DataManager 数据管理器
type DataManager struct {
    operateLock   sync.RWMutex         // 事务锁，控制外部对manager的操作
    dataStore     IStore               // 索引值 -> 数据的存储接口
    fieldMap      InvertFieldMap       // 索引字段名 -> (索引值 -> 数据)
    indexToBitmap IIdToBitmap          // 索引值 -> Bitmap的关系存储
    indexInfo     *structIndexInfo     // 结构体的索引信息
    invertFilter  IInvertBuildFilter   // 索引构建过滤器
    slowQueryLog  SlowQueryLog         // 慢查询回调
}

func NewDataManager(filters ...IInvertBuildFilter) *DataManager {
    return NewDataManagerEx(NewNogcMap(), NewIdToBitmap(), filters...)
}

func NewDataManagerEx(store IStore, i2b IIdToBitmap, filters ...IInvertBuildFilter) *DataManager {
    if store == nil {
        panic("DataManager: input IStore is nil")
    }
    if i2b == nil {
        panic("DataManager: input IIdToBitmap is nil")
    }

    dm := &DataManager{}
    dm.init(store, i2b)

    // 设置倒排索引构建规则
    if len(filters) > 0 {
        dm.SetInvertFilter(filters[0])
    }
    return dm
}

// IsErrNoData 判断错误是否为ErrNoData
func IsErrNoData(err error) bool {
    if err == nil {
        return false
    }
    return errors.Is(err, ErrNoData)
}

// SetInvertFilter 设置数据的倒排索引构建过滤器
func (receiver *DataManager) SetInvertFilter(filter IInvertBuildFilter) {
    receiver.invertFilter = filter
}

// SetSlowQueryLog 设置查询过程中未命中索引(全表扫描)时的回调
func (receiver *DataManager) SetSlowQueryLog(log SlowQueryLog) {
    receiver.slowQueryLog = log
}

// GetStore 返回数据管理器中的存储引擎
func (receiver *DataManager) GetStore() IStore {
    return receiver.dataStore
}

// RebuildInvert 重新构建主键索引对应数据的倒排索引
func (receiver *DataManager) RebuildInvert(index DataIndex) error {
    receiver.Lock()
    defer receiver.Unlock()

    return receiver.rebuildInvert(index)
}

// Insert 插入数据指针, 主键索引必须为Integer
func (receiver *DataManager) Insert(data AnyData) (ret error) {
    _, err := receiver.InsertAndReturnIndex(data)
    return err
}

// InsertAndReturnIndex 插入数据指针并返回主键索引, 主键索引必须为Integer
func (receiver *DataManager) InsertAndReturnIndex(data AnyData) (index DataIndex, ret error) {
    receiver.Lock()
    defer receiver.Unlock()

    return receiver.insertAndReturn(data)
}

// Replace 替换数据
func (receiver *DataManager) Replace(data AnyData) (ret error) {
    receiver.Lock()
    defer receiver.Unlock()

    return receiver.replace(data)
}

// Update 修改数据字段的值
func (receiver *DataManager) Update(index DataIndex, fieldMap FieldMap) error {
    _, err := receiver.UpdateAndReturn(index, fieldMap)
    return err
}

// UpdateAndReturn 修改数据字段的值并返回数据指针
func (receiver *DataManager) UpdateAndReturn(index DataIndex, fieldMap FieldMap) (data AnyData, ret error) {
    receiver.Lock()
    defer receiver.Unlock()

    return receiver.updateAndReturn(index, fieldMap)
}

// ModifyDataByIndex 根据主键索引修改数据 9m
// 删除对应数据全部倒排索引
// 通过modifier进行数据修改
// 构建对应数据全部倒排索引
// 适用于倒排索引数量少的数据
func (receiver *DataManager) ModifyDataByIndex(modifier DataModifier, index DataIndex) error {
    receiver.Lock()
    defer receiver.Unlock()

    return receiver.modifyDataByIndex(nil, modifier, index)
}

// ModifyDataByIndexEx 根据主键索引修改数据(优化增强版)
// 通过prepar返回待修改的倒排字段
// 删除对应数据待修改的倒排索引
// 通过modifier进行数据修改
// 构建对应数据字段的倒排所索引
// 适用于倒排索引数量多的数据
// **风险** prepare返回的字段与modifier修改的字段不一致, 会导致倒排索引部分错误
func (receiver *DataManager) ModifyDataByIndexEx(prepare PrepareModify, modifier DataModifier, index DataIndex) error {
    receiver.Lock()
    defer receiver.Unlock()

    return receiver.modifyDataByIndex(prepare, modifier, index)
}

// Delete 删除主键索引对应的数据记录
func (receiver *DataManager) Delete(index DataIndex) error {
    receiver.Lock()
    defer receiver.Unlock()

    return receiver.delete(index)
}

// // DeleteAndReturn 删除主键索引对应的数据记录并返回被删除的数据(如果成功)
// func (receiver *DataManager) DeleteAndReturn(index DataIndex) (AnyData, error) {
//     receiver.Lock()
//     defer receiver.Unlock()
//
//     return receiver.deleteAndReturn(index)
// }

func (receiver *DataManager) QueryByIndex(indexs ...DataIndex) IQueryResult {
    receiver.RLock()
    defer receiver.RUnlock()

    return receiver.queryByIndex(indexs...)
}

func (receiver *DataManager) QueryByFields(fieldMap FieldMap) IQueryResult {
    receiver.RLock()
    defer receiver.RUnlock()

    return receiver.queryByFields(fieldMap)
}

func (receiver *DataManager) QueryIdsByFields(fieldMap FieldMap) IQueryResult {
    receiver.RLock()
    defer receiver.RUnlock()

    return receiver.queryIdsByFields(fieldMap)
}

// Size 存储的数据数量
func (receiver *DataManager) Size() int {
    receiver.RLock()
    defer receiver.RUnlock()

    return receiver.dataStore.Size()
}

// Count 存储的数据数量 (Size的别名)
func (receiver *DataManager) Count() int {
    return receiver.Size()
}

// BytesSize 内存使用大小
func (receiver *DataManager) BytesSize() int {
    return receiver.dataStore.Capacity()
}

// BitmapCount bitmap总数量和每个倒排字段申请的bitmap数量
func (receiver *DataManager) BitmapCount() (total uint64, fields map[string]uint64) {
    receiver.RLock()
    defer receiver.RUnlock()

    return receiver.bitmapCount()
}

// BitmapCapacity bitmap总内存占用和每个倒排字段的bitmap占用内存总量
func (receiver *DataManager) BitmapCapacity() (total Stats, fields map[string]*Stats) {
    receiver.RLock()
    defer receiver.RUnlock()

    return receiver.bitmapCapacity()
}

// IdToBtmStats 获取数据管理器内部的IdToBitmap统计信息
func (receiver *DataManager) IdToBtmStats() IToBStats {
    receiver.RLock()
    defer receiver.RUnlock()

    return receiver.idToBtmStats()
}

// IterateData TODO 迭代元素方法, 待实现
func (receiver *DataManager) IterateData() {

}