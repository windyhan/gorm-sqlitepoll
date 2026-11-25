# 单文件版

一个 `.go` 文件就是一个完整的包：没有子包、没有驱动注册、没有 build tag、不用改任何业务代码。

```
standalone/
├── purego/gormsqlite.go   纯 Go（modernc），CGO_ENABLED=0 可编，任何架构
└── cgo/gormsqlite.go      cgo（mattn），性能最好，部署机要有 gcc
```

**复制其中一个**到你项目的 `internal/gormsqlite/`（或任何独立目录），保持 `package gormsqlite`
不变，就能用了。两个文件的对外接口**完全一样**，换过去不用改调用代码。

## 一个文件只能绑一个驱动

这是硬约束，不是设计选择：

| 驱动 | GORM 方言 | 是否要 cgo |
|---|---|---|
| mattn/go-sqlite3 | `gorm.io/driver/sqlite` | **要**（这个方言包自己 import 了 mattn） |
| modernc | `github.com/glebarez/sqlite` | 不要 |

`gorm.io/driver/sqlite` 一旦被 import，`CGO_ENABLED=0` 就废了。所以两个驱动无法共存于
同一个文件，只能二选一。

## 选哪个

| | purego | cgo |
|---|---|---|
| 编译 | `CGO_ENABLED=0` 直接过 | 要 gcc + C 工具链 |
| 交叉编译 | `GOOS/GOARCH` 随便换 | 要配 `CC=aarch64-linux-gnu-gcc` 之类 |
| 纯读 QPS（1 核 / 4 核） | 24,649 / 75,732 | **27,519 / 101,981** |
| 混合负载 | −3.3% ~ −9.4% | 基准 |
| 1 核 VPS 写 | 642/s | **773/s** |

- 部署机没有 gcc、或者要出静态二进制、或者目标架构不好配工具链（比如 ARM64 小机器）→ **purego**
- 机器上本来就能装 gcc、想榨性能 → **cgo**

> 踩过的坑：cgo 版在 `CGO_ENABLED=0` 下**编译能过但运行时报错**
> （`Binary was compiled with 'CGO_ENABLED=0', go-sqlite3 requires cgo to work. This is a stub`）。
> 不要以为编译过就没事，一定要跑一次。

## 依赖

```bash
# purego
go get gorm.io/gorm gorm.io/plugin/dbresolver github.com/glebarez/sqlite

# cgo
go get gorm.io/gorm gorm.io/plugin/dbresolver gorm.io/driver/sqlite github.com/mattn/go-sqlite3
```

`gorm.io/plugin/dbresolver` 是 GORM 官方插件，负责"Create/Update/Delete 走写池、
Query 走读池、事务内不切换"的路由。不引它就得自己写回调，容易在边界上出错。

## 用法

```go
import "你的模块/internal/gormsqlite"

db, err := gormsqlite.Open(gormsqlite.Config{
    Path:  "data/app.db",
    Debug: true, // 打印选中的拓扑和实际生效的 PRAGMA
})
if err != nil {
    log.Fatal(err)
}
defer db.Close()

db.AutoMigrate(&User{})   // DDL 走写池
db.Create(&user)          // 写池
db.First(&user, 1)        // 读池
```

`Debug: true` 的输出：

```
gormsqlite: path=data/app.db mode=split（GOMAXPROCS=2，Auto 自动选择）driver=modernc
  写池 MaxOpen=1  journal=wal synchronous=FULL busy_timeout=5000
  读池 MaxOpen=2  journal=wal synchronous=FULL busy_timeout=5000 query_only=1
```

## 验证

```bash
go test -count=1 ./internal/gormsqlite
```

两个目录里都带了一份 `gormsqlite_test.go`（4 个用例）：单连接 CRUD、分离路由与读池拒写、
Auto 在 1/2 核的选择、checkpoint 与重复 Close。复制 `.go` 时可以一起带上，跑通再删。

## 与多文件版的差别

`../gormsqlite.go` + `../driver/*` 那套是"多驱动 + 运行时注册"，可以在一个二进制里同时
链接两个驱动、用 `Config.Driver` 切换。单文件版去掉了这层抽象，换来零结构负担。

功能上除了驱动切换，其余完全一致：两种拓扑、`Stats`、`WALSize`、`Checkpoint`、`Config`、
`Close` 的关库顺序都一样。
"# gorm-sqlitepoll" 
