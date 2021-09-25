package dm

import (
    "fmt"
    "math/rand"
    "testing"
    "time"

    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/runtime"
    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/utils"
)

type smallStruct struct {
    Idx  int `dm:"index"`
    SId  int `dm:"invert"`
    Name string
}

type largeStruct struct {
    Id                    int       `json:"id" dm:"index"`
    Name                  string    `json:"name"`
    Instructions          string    `json:"instructions"`
    Notice                string    `json:"notice"`
    LiveType              int       `json:"liveType"`
    TeacherId             int       `json:"teacherId" dm:"invert"`
    LiveChatRoom          int       `json:"-"`
    CourseWareId          int       `json:"coursewareId" dm:"invert"`
    IsSeries              int       `json:"isBelongToSeries" dm:"invert"`
    SeriesLectureId       int       `json:"seriesId" dm:"invert"`
    Day                   string    `json:"-"`
    StartTime             time.Time `json:"-"` // 提前15分钟
    StartTimeStr          string    `json:"-"`
    EndTime               time.Time `json:"-"` //延后半小时
    EndTimeStr            string    `json:"-"`
    StartTimeUnix         int64     `json:"stime" dm:"invert"`
    EndTimeUnix           int64     `json:"etime" dm:"invert"`
    GotoStartTime         string    `json:"-"`
    GotoEndTime           string    `json:"-"`
    GradeIds              string    `json:"grade_ids"`
    GradeIdsStr           string    `json:"gradeIds"`
    GradeIdList           []int     `json:"-"`
    SubjectIds            string    `json:"subject_ids"`
    SubjectIdsStr         string    `json:"subjectIds"`
    SubjectIdList         []int     `json:"-"`
    SubjectDigits         int       `json:"-"`
    Status                int       `json:"status"`        //返回给前端的状态
    OriginStatus          int       `json:"origin_status"` //数据库原生状态
    Recommend             int       `json:"-"`
    PlaybackRecommend     int       `json:"-"`
    LivePlaybackRecommend int       `json:"-"`
    OperationDepartment   int       `json:"-"`
    ImageUrl              string    `json:"imageUrl"`          //讲座学科小图
    BigImageUrl           string    `json:"bigImageUrl"`       //讲座学科大图
    IsBespoke             int       `json:"-"`                 //是否预约
    ScheduleTime          string    `json:"scheduleTime"`      //讲座时间 06月17日 10:30-12:00
    StudyreportStatus     int       `json:"studyreportStatus"` //学习报告
    StudyreportUrl        string    `json:"studyreportUrl"`
    TeacherName           string    `json:"teacherName"`      //教师名称
    TeacherImg            string    `json:"teacherImg"`       //教师头像
    TeacherSpell          string    `json:"teacherSpell"`     //教师拼音名
    LiveNum               int       `json:"liveNum"`          //在学人数
    ReservationNum        int       `json:"reservationNum"`   //预约人数
    VisitNum              int       `json:"visitNum"`         //学完人数
    PlaybackIsLogin       int       `json:"playback_islogin"` //观看回放是否需要登录
    AdvertiseIds          string    `json:"advertise_ids"`    //关联的广告ID
    ActivityUrl           string    `json:"-"`
    IsProgram             int       `json:"isProgram"`
    PlayProtocol          int       `json:"-"`
    StatusByTime          int       `json:"-"`      // unStart 2, start 3, end 4
    IsGray                int       `json:"isGray"` //是否灰度讲座
    TestIds               string    `json:"testIds"`
    H5SourceIds           string    `json:"h5SourceIds"` //关联的H5素材ID
    ProgrammingUrl        string    `json:"ProgrammingUrl"`
    IsGently              int       `json:"isGently"`             //是否为轻直播（0 否  1 是）
    TicketIds             string    `json:"ticketIds"`            //优惠券id list
    CourseIds             string    `json:"courseIds"`            //课程id list
    GentlyNotice          string    `json:"gentlyNotice"`         //轻直播公告
    IsNeedLogin           int       `json:"isNeedLogin"`          //是否需要登录
    OfficialAccount       string    `json:"officialAccount"`      //公众号
    TeachingVersionIds    string    `json:"teachingVersionIds"`   //教学版本ids
    LiveRoomType          int       `json:"liveRoomType"`         //直播间类型
    FutureCoursewareId    int       `json:"future_courseware_id"` //未来课件id
    CreatorId             int       `json:"creatorId"`            // 未来号
    LoginCheckType        int       `json:"loginCheckType"`       //登录验证方式
    GoingId               int       `json:"goingId"`              //新版学习中心---系列讲座中正在直播的场次id
    FristView             int       `json:"fristView"`            //是否上首页
    SortWeight            int       `json:"sortWeight"`           //首页权重
    CoverPicture          string    `json:"coverPicture"`         //首页封面图
    UvNum                 int64     `json:"UvNum"`
    StuNum                int64     `json:"StuNum"`
    PlaybackUvNum         int64     `json:"PlaybackUvNum"`
    PlaybackVisitNum      int64     `json:"PlaybackVisitNum"`
    WxType                int       `json:"wxType"` //微信按钮类型
    ExistsPlayback        int       `json:"existsPlayback"`
    HotId                 int       `json:"hotId"`           // 热点ID
    IsOpenGraffiti        int       `json:"-"`               // 是否开启涂鸦插件
    IsIpad                int       `json:"-"`               // 是否iPad直播
    PlaybackChatPic       string    `json:"playbackChatPic"` // 回放聊天区遮罩图片url
    AppIsHandstand        int       `json:"-"`               // 是否竖版直播
    LiveScene             int       `json:"-"`               // 直播场景
    CourseListImg         string    `json:"-"`               // 课表页展示图
    Cover                 string    `json:"cover"`
    CoverWidth            int       `json:"cover_width"`
    CoverHeight           int       `json:"cover_height"`
    Cover32               string    `json:"cover_32"`
    Cover32Width          int       `json:"cover_32_width"`
    Cover32Height         int       `json:"cover_32_height"`
    ProvinceIds           string    `json:"province_ids"`
    HomeShowType          string    `json:"home_show_type"`
    Portrait              string    `json:"portrait"`
    LectureAuthorType     int       `json:"lecture_author_type"`
    ExtSourceId           int       `json:"ext_source_id"`
    SourceType            int       `json:"source_type"`
    VersionIds            string    `json:"version_ids"`
    IsDisplay             int       `json:"is_display"`
    ContentPoolType       int       `json:"content_pool_type"`
    ProvinceIdsList       []int     `json:"-"`
    ClassId               int       `json:"class_id"`
    IsMoreMic             int       `json:"is_more_mic"`
    IsClassGray           int       `json:"is_class_gray"`
}

var dm *DataManager

// count    数据量
// disperse 数据分散度, 分散度越小, 获取数据越多
func initLargeDataManager(count, disperse int) *DataManager {
    if dm == nil {
        start := time.Now()

        dm = NewDataManager()

        for i := 0; i < count; i++ {
            l := largeStruct{
                Id:            i,
                SubjectIdsStr: fmt.Sprintf("SID%d", i),
                CourseWareId:  rand.Int() % disperse,
            }
            if err := dm.Insert(&l); err != nil {
                panic(err)
            }
        }

        elapsed := time.Since(start)
        fmt.Printf("%s count:%d disperse:%d cost:%v\n", runtime.CallerFunction(), count, disperse, elapsed)
    }
    return dm
}

// 小结构体insert
// BenchmarkDataManager_Insert-8     834628      1507 ns/op     309 B/op      12 allocs/op
// BenchmarkDataManager_Insert-8     446614      3291 ns/op    4629 B/op      18 allocs/op  使用NogcMap
func BenchmarkDataManager_Insert(b *testing.B) {
    m := NewDataManager()

    for i := 0; i < b.N; i++ {
        s := smallStruct{
            Idx:  i,
            SId:  rand.Int() % 100,
            Name: fmt.Sprintf("A%d", i),
        }

        if err := m.Insert(&s); err != nil {
            b.Fatal(err)
        }
    }
}

// 大结构体insert
// BenchmarkDataManager_Insert1-8     380978      3430 ns/op    1855 B/op      27 allocs/op
// BenchmarkDataManager_Insert1-8     155946     10767 ns/op    8192 B/op      38 allocs/op  使用NogcMap
func BenchmarkDataManager_Insert1(b *testing.B) {
    m := NewDataManager()

    for i := 0; i < b.N; i++ {
        l := largeStruct{
            Id:            i,
            SubjectIdsStr: fmt.Sprintf("SID%d", i),
        }

        if err := m.Insert(&l); err != nil {
            b.Fatal(err)
        }
    }
}

// BenchmarkMapStruct-8     917892      1369 ns/op    1396 B/op       3 allocs/op
func BenchmarkMapStruct(b *testing.B) {
    b.ReportAllocs()

    x := make(map[int]largeStruct)
    for i := 0; i < b.N; i++ {
        l := largeStruct{
            Id:            i,
            SubjectIdsStr: fmt.Sprintf("SID%d", i),
        }
        x[l.Id] = l
    }
}

// BenchmarkMapPointer-8     960402      1315 ns/op    1392 B/op       3 allocs/op
func BenchmarkMapPointer(b *testing.B) {
    b.ReportAllocs()

    x := make(map[int]*largeStruct)
    for i := 0; i < b.N; i++ {
        l := largeStruct{
            Id:            i,
            SubjectIdsStr: fmt.Sprintf("SID%d", i),
        }
        x[l.Id] = &l
    }
}

// 大结构体Get
// BenchmarkDataManager_GetDataByFields-8     111164     11715 ns/op    6240 B/op     267 allocs/op
func BenchmarkDataManager_GetDataByFields(b *testing.B) {
    initLargeDataManager(1000000, 10000)
    b.ResetTimer()

    fieldMap := FieldInfosToFieldMap(FieldInfo{"CourseWareId", 20})
    for i := 0; i < b.N; i++ {
        result := dm.QueryByFields(fieldMap)
        _, err := result.GetAll()
        if err != nil {
            b.Fatal(err)
        }
    }
}

// 大结构体Modify
// BenchmarkDataManager_Modify-8     231618      5265 ns/op     860 B/op      49 allocs/op
func BenchmarkDataManager_Modify(b *testing.B) {
    m := initLargeDataManager(1000000, 10000)
    b.ResetTimer()

    for i := 0; i < b.N; i++ {
        err := m.ModifyDataByIndex(func(dataPtr AnyData) error {
            if value, ok := dataPtr.(*largeStruct); ok {
                value.SubjectIdsStr = fmt.Sprintf("SID%d", i+1)
            }
            return nil
        }, 100)

        if err != nil {
            b.Fatal(err)
        }
    }
}

// BenchmarkClone_LargeStruct-8    6052    191349 ns/op   79961 B/op    1087 allocs/op
func BenchmarkClone_LargeStruct10(b *testing.B) {
    l1 := []largeStruct{}
    for i := 0; i < 10; i++ {
        l1 = append(l1, largeStruct{Id:i, Name: fmt.Sprintf("TTTTTTTTTTTTTTTT%d", i)})
    }

    for i := 0; i < b.N; i++ {
        var l2 []largeStruct

        err := utils.Clone(l1, &l2)
        if err != nil {
            b.Fatal(err)
        }
    }
}

// BenchmarkClone_LargeStruct-8     58    20216675 ns/op   8938657 B/op    107070 allocs/op
func BenchmarkClone_LargeStruct1000(b *testing.B) {
    l1 := []largeStruct{}
    for i := 0; i < 1000; i++ {
        l1 = append(l1, largeStruct{Id:i, Name: fmt.Sprintf("TTTTTTTTTTTTTTTT%d", i)})
    }

    for i := 0; i < b.N; i++ {
        var l2 []largeStruct

        err := utils.Clone(l1, &l2)
        if err != nil {
            b.Fatal(err)
        }
    }
}

// BenchmarkClone_LargeStruct1000_Split-8   43   29135096 ns/op     15468129 B/op  114062 allocs/op
func BenchmarkClone_LargeStruct1000_Split(b *testing.B) {
    l1 := []largeStruct{}
    for i := 0; i < 1000; i++ {
        l1 = append(l1, largeStruct{Id:i, Name: fmt.Sprintf("TTTTTTTTTTTTTTTT%d", i)})
    }

    l2 := make([]largeStruct, 0, len(l1))

    for i := 0; i < b.N; i++ {
        for _, data := range l1 {
            l3 := largeStruct{}
            err := utils.Clone(data, &l3)
            if err != nil {
                b.Fatal(err)
            }

            l2 = append(l2, l3)
        }
    }
}