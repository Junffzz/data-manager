package dm

import (
    "time"
    "unsafe"

    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/utils"
)

// KeyType 存储的Key类型
type KeyType = int

// Filter 数据过滤函数, 循环被回调 (场景: 数据量很大, 但是只使用其中的子集, 防止Clone耗时)
// exclude: 数据排除
// err:     产生的错误信息 -> 错误导致整个过滤过程失效
type Filter func(dataPtr AnyData) (exclude bool, err error)

type FilterUnsafe func(dataPtr unsafe.Pointer) (exclude bool, err error)

// Comparer 数据比较函数
// iObj: slice中第i个对象
// jObj: slice中第j个对象
type Comparer = utils.AnyCompare

// QueryCostDetail 查询过程中的耗时信息
type QueryCostDetail struct {
    Cost   time.Duration        // 查询耗时
    IsSlow bool                 // 是否为慢查询
    Miss   FieldMap             // 未命中索引的字段
}

// IQueryResult 查询结果集
type IQueryResult interface {
    ICustomQueryResult

    /************* 数据操作类 *************/

    Sift(f Filter) IQueryResult                            // 数据过滤, 大集合通过f返回小集合
    Sort(c Comparer) IQueryResult                          // 数据排序, 通过c进行数据比较
    Slice(s, e int) IQueryResult                           // 数据切片, [s:Start e:End)
    Page(cur, per int) IQueryResult                        // 数据分页, [(cur-1)*per : (cur-1)*per+per)
    Reverse() IQueryResult                                 // 数据反转

    /************* 数据获取类 *************/

    FetchCost(cost *time.Duration) IQueryResult            // 获取查询耗时
    FetchCostDetail(detail *QueryCostDetail) IQueryResult  // 获取查询耗时细节
    FetchCount(size *int) IQueryResult                     // 获取数据数量
    FetchOne(bean AnyData) IQueryResult                    // 获取单条数据, 数据拷贝到bean, bean必须与原始数据类型一致
    FetchSome(beans AnyDatas, s, e int) IQueryResult       // 获取多条数据, 与Slice().FetchAll的区别: 不修改结果集 -> 只获取结果集的一部分
    FetchAll(beans AnyDatas) IQueryResult                  // 获取全部数据, 数据拷贝到beans(Slice), beans的元素类型必须与原始数据类型一致
    FetchMap(beansMap AnyData) IQueryResult                // 获取全部数据, 数据拷贝到beans(Map[index]data), 元素类型必须与原始数据类型一致
    FetchIndexs(indexs AnyDatas) IQueryResult              // 获取全部索引数据

    /************* 信息获取类 *************/

    GetSize() int                                          // 数据数量
    GetError() error                                       // 获取链式调用过程中的错误
    GetOne() (AnyData, error)                              // 获取单条数据, 数据为原始数据指针 (注:数据操作存在并发问题)
    GetAll() ([]AnyData, error)                            // 获取多条数据, 数据为原始数据指针 (注:数据操作存在并发问题)
}

// ICustomQueryResult 自定义的查询结果集
type ICustomQueryResult interface {
    SetError(err error)
    SetResult(result IQueryResult)
}

// IInvertBuildFilter 倒排索引构建过滤器
type IInvertBuildFilter interface {
    // FieldInvertFilter 单个字段进行倒排索引构建时调用 -> 返回是否构建此字段的倒排索引
    // build == true  -> 构建<fieldName>字段值为<fieldValue>的倒排索引
    // build == false -> 不构建<fieldName>字段值为<fieldValue>的倒排索引
    FieldInvertFilter(data AnyData, fieldName string, fieldValue AnyData) (build bool)

    // FieldInvertFilterMap 返回字段倒排索引过滤字典
    // filter == nil -> 不进行过滤
    // filter != nil -> 未在fitler中出现的fieldName -> 进行过滤s
    FieldInvertFilterMap(data AnyData) (filter FieldMap)
}

// IStore DataManager底层存储接口
type IStore interface {
    // Size 获取数据数量
    Size() int

    // HasKey 检测key是否存在
    HasKey(key KeyType) bool

    // Keys 返回key的列表
    Keys() []KeyType

    // Get 通过key获取value
    Get(key KeyType) (value interface{}, err error)

    // Set 设置key, value
    Set(key KeyType, value interface{}) error

    // Del 删除key对应的value
    Del(key KeyType) (has bool)

    // Fetch 获取key对应的value的拷贝
    Fetch(key KeyType, valuePtr interface{}) error

    // Iterator 迭代遍历
    // iter 返回true: 继续遍历 false: 停止遍历
    Iterator(iter func(keyType KeyType, value interface{}) bool) error

    // Capacity 获取存储容量
    Capacity() int
}

// IStoreBackfill Store实现此接口来支持多级Store的回填
type IStoreBackfill interface {
    BackFill(key KeyType, value interface{}) error
}

type IStoreExpand interface {
    // GetUnsafe 通过key获取value,非安全方式，持有内部指针
    GetUnsafe(key KeyType) (value unsafe.Pointer, err error)
}
