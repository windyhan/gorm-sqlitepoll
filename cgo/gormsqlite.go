// Package gormsqlite 在单个 SQLite 文件上提供两种连接拓扑，调用方仍然只用一个
// *gorm.DB、只写 GORM 原生方法。这是**单文件版（cgo / mattn 驱动）**。
//
// 依赖（三条命令，或者直接 go mod tidy）：
//
//	go get gorm.io/gorm
//	go get gorm.io/plugin/dbresolver
//	go get gorm.io/driver/sqlite
//	go get github.com/mattn/go-sqlite3
//
// 需要 cgo 和 C 编译器。交叉编译要配 CGO_ENABLED=1 加对应工具链
// （例如 ARM64：CC=aarch64-linux-gnu-gcc CGO_ENABLED=1 GOARCH=arm64 go build）。
//
// 如果你的部署环境没有 gcc、或者想 CGO_ENABLED=0 出静态二进制，
// 用同目录的纯 Go 版本（standalone/purego/gormsqlite.go），代价是慢 3%~26%。
//
// 用法：把本文件放进你项目的一个目录（例如 internal/db/），保持 package 名不变，然后：
//
//	db, err := gormsqlite.Open(gormsqlite.Config{Path: "data/app.db"})
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer db.Close()
//
//	db.AutoMigrate(&User{})
//	db.Create(&user)                                          // 走写池
//	db.First(&user, 1)                                        // 走读池
//	db.Model(&User{}).Where("id = ?", 1).Update("name", "x")   // 走写池
//	db.Transaction(func(tx *gorm.DB) error {                   // 事务内全走写池
//		return tx.Create(&order).Error
//	})
//
// # 两种拓扑
//
//	Single  1 个池、1 条连接，读写共用。适合 1 核机器。
//	Split   写池 1 条连接 + 读池 N 条连接，指向同一个文件。适合 2 核及以上。
//
// Mode 默认 Auto：GOMAXPROCS==1 用 Single，>=2 用 Split。
//
// 实测（Windows，WAL+FULL，30s，80% 读 / 20% 写）：
//
//	1 核  Single 11,471 QPS  /  Split 10,484 QPS（−8.6%）
//	2 核  Single  9,335 QPS  /  Split 15,990 QPS（+71%，读 p50 1,159μs → 38μs）
//	4 核  Single  8,806 QPS  /  Split 16,242 QPS（只比双核 +1.6%）
package gormsqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
	"gorm.io/plugin/dbresolver"
)

// Mode 是连接拓扑。
type Mode int

const (
	// Auto 按核数自动选择：GOMAXPROCS==1 用 Single，>=2 用 Split。
	Auto Mode = iota
	// Single 只开一个池、只有 1 条连接，读写共用。单核机器的最优解。
	Single
	// Split 写池 1 条连接 + 读池 N 条连接，指向同一个文件。多核机器的最优解。
	Split
)

func (m Mode) String() string {
	switch m {
	case Single:
		return "single"
	case Split:
		return "split"
	default:
		return "auto"
	}
}

const (
	// 读连接数上限。实测 4 条已能喂满 4 核，再加只增加内存不增加吞吐。
	maxReadConns = 8
	// 默认每连接页缓存（MB）。总内存约等于 (读连接数+1) * CacheMB。
	defaultCacheMB = 16
	// 默认 busy 超时。写池只有 1 条连接，锁等待全部发生在 SQLite 内部。
	defaultBusyTimeout = 5 * time.Second
)

// Config 是打开数据库的配置。除 Path 外都有合理默认值。
type Config struct {
	// Path 是 SQLite 数据库文件路径。必填。传 ":memory:" 会强制退化为 Single。
	Path string

	// Mode 是连接拓扑，默认 Auto（1 核 Single，>=2 核 Split）。
	Mode Mode

	// ReadConns 是 Split 模式下读池的连接数，Single 模式下会被忽略。
	// 0 表示自动：等于 GOMAXPROCS，上限 8。
	ReadConns int

	// Synchronous 是 synchronous 级别，默认 "FULL"。
	// 想用 NORMAL 换吞吐要清楚代价：提交不再 fsync，断电可能丢最近若干事务。
	Synchronous string

	// BusyTimeout 是锁等待超时，默认 5s。
	BusyTimeout time.Duration

	// CacheMB 是每条连接的页缓存大小，默认 16MB。
	CacheMB int

	// SkipDefaultTransaction 关掉 GORM 的"每条写入包一层事务"。
	// 单条语句写入能快约 10%，但带关联的多语句写入会失去原子性。
	SkipDefaultTransaction bool

	// Logger 透传给 GORM。nil 表示用 GORM 默认日志。
	Logger gormlogger.Interface

	// Debug 打开后会在 Open 时打印选中的拓扑和实际生效的 PRAGMA。
	Debug bool

	// ExtraDSN 是额外的 DSN 参数，例如 map[string]string{"_foreign_keys": "on"}。
	// 注意本驱动（mattn）用 _foreign_keys=on 这种独立键，不是 _pragma=... 语法。
	ExtraDSN map[string]string
}

// DB 包装一个 *gorm.DB。由于是嵌入，所有 GORM 方法（Create/Find/Where/Transaction/...）
// 都可以直接在 *DB 上调用，用法和原生 GORM 完全一致。
type DB struct {
	*gorm.DB

	mode  Mode
	auto  bool // mode 是否由 Auto 推断而来（只影响日志措辞）
	cfg   Config
	write *sql.DB
	read  *sql.DB
	once  sync.Once
	err   error
}

// Open 打开数据库。写池会先于读池建立并 Ping 一次，
// 这样 WAL 的恢复动作由可写连接完成，不会让只读连接撞上 "attempt to write a readonly database"。
func Open(cfg Config) (*DB, error) {
	if strings.TrimSpace(cfg.Path) == "" {
		return nil, errors.New("gormsqlite: Path 不能为空")
	}
	cfg = withDefaults(cfg)

	mode := cfg.Mode
	auto := mode == Auto
	if isMemory(cfg.Path) {
		// 内存库每条连接是一个独立的库，分离没有意义，强制单连接。
		mode = Single
		auto = false
	} else if auto {
		if runtime.GOMAXPROCS(0) >= 2 {
			mode = Split
		} else {
			mode = Single
		}
	}

	write, err := openPool(cfg, 1, false)
	if err != nil {
		return nil, fmt.Errorf("gormsqlite: 打开写池失败(path=%s): %w", cfg.Path, err)
	}

	writeDial := sqlite.New(sqlite.Config{Conn: write})
	gdb, err := gorm.Open(writeDial, &gorm.Config{
		SkipDefaultTransaction: cfg.SkipDefaultTransaction,
		Logger:                 cfg.Logger,
	})
	if err != nil {
		write.Close()
		return nil, err
	}

	d := &DB{DB: gdb, mode: mode, auto: auto, cfg: cfg, write: write, read: write}

	if mode == Split {
		read, err := openPool(cfg, cfg.ReadConns, true)
		if err != nil {
			write.Close()
			return nil, fmt.Errorf("gormsqlite: 打开读池失败(path=%s): %w", cfg.Path, err)
		}
		d.read = read

		resolver := dbresolver.Register(dbresolver.Config{
			Sources:  []gorm.Dialector{writeDial},
			Replicas: []gorm.Dialector{sqlite.New(sqlite.Config{Conn: read})},
			Policy:   dbresolver.RandomPolicy{},
		})
		if err := gdb.Use(resolver); err != nil {
			read.Close()
			write.Close()
			return nil, err
		}
	}

	if cfg.Debug {
		d.logConfig()
	}
	return d, nil
}

// MustOpen 同 Open，出错直接 panic。
func MustOpen(cfg Config) *DB {
	d, err := Open(cfg)
	if err != nil {
		panic(err)
	}
	return d
}

// EffectiveMode 返回实际生效的拓扑（Auto 会被解析成 Single 或 Split）。
func (d *DB) EffectiveMode() Mode { return d.mode }

// Config 返回补齐默认值之后的配置。
func (d *DB) Config() Config { return d.cfg }

// Pools 返回底层两个连接池。Single 模式下两者是同一个。
func (d *DB) Pools() (write, read *sql.DB) { return d.write, d.read }

// Stats 是运行时可观测数据，不含任何 I/O，可以高频调用。
//
// Split 模式下重点看 Read.WaitCount：它统计"读请求因为读池没空闲连接而排队"的次数。
// 随 QPS 一起涨说明 ReadConns 给少了；一直是 0 说明给多了。
type Stats struct {
	Mode      Mode
	ReadConns int
	Write     sql.DBStats
	Read      sql.DBStats
}

// Stats 返回两个池的统计。Single 模式下 Write 与 Read 是同一份数据。
func (d *DB) Stats() Stats {
	readConns := d.cfg.ReadConns
	if d.mode != Split {
		readConns = 1 // 没有独立读池，读走的就是那唯一一条连接
	}
	return Stats{Mode: d.mode, ReadConns: readConns, Write: d.write.Stats(), Read: d.read.Stats()}
}

// WALSize 返回 -wal 文件的字节数。文件不存在时返回 0。
//
// 注意文件大小 ≠ 未回写数据量：SQLite 攒够 1000 页会把 WAL 重置回第 0 帧但不缩小文件。
func (d *DB) WALSize() (int64, error) {
	if isMemory(d.cfg.Path) {
		return 0, nil
	}
	fi, err := os.Stat(d.cfg.Path + "-wal")
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// CheckpointMode 是 PRAGMA wal_checkpoint 的模式。
type CheckpointMode string

const (
	// CheckpointPassive 不阻塞任何读写，能搬多少搬多少（SQLite 自动做的就是它）。
	CheckpointPassive CheckpointMode = "PASSIVE"
	// CheckpointFull 等所有读事务结束，把 WAL 全部回写，但不动文件大小。
	CheckpointFull CheckpointMode = "FULL"
	// CheckpointRestart 在 FULL 基础上让下一次写从 WAL 头部重新开始。
	CheckpointRestart CheckpointMode = "RESTART"
	// CheckpointTruncate 在 RESTART 基础上把 WAL 截断为 0 字节。关库时用这个。
	CheckpointTruncate CheckpointMode = "TRUNCATE"
)

// CheckpointResult 对应 PRAGMA wal_checkpoint 返回的三列。
type CheckpointResult struct {
	// Busy 为 true 表示有读事务挡住了 checkpoint，这次没做完。
	Busy bool
	// LogFrames 是 WAL 里已有的帧数。
	LogFrames int
	// Checkpointed 是本次成功回写到主库的帧数。
	Checkpointed int
}

// Checkpoint 手动执行一次 WAL 回写。传空字符串等价于 CheckpointPassive。
// TRUNCATE 会短暂阻塞写入，不要在高峰期用。
func (d *DB) Checkpoint(mode CheckpointMode) (CheckpointResult, error) {
	var res CheckpointResult
	switch mode {
	case "":
		mode = CheckpointPassive
	case CheckpointPassive, CheckpointFull, CheckpointRestart, CheckpointTruncate:
	default:
		return res, fmt.Errorf("gormsqlite: 未知的 checkpoint 模式 %q", string(mode))
	}
	if isMemory(d.cfg.Path) {
		return res, nil // 内存库没有 WAL 文件
	}

	var busy, logFrames, ckpt int
	// mode 已白名单校验，不存在注入面。
	if err := d.write.QueryRow("PRAGMA wal_checkpoint("+string(mode)+")").Scan(&busy, &logFrames, &ckpt); err != nil {
		return res, err
	}
	res.Busy = busy != 0
	res.LogFrames = logFrames
	res.Checkpointed = ckpt
	return res, nil
}

// Close 关闭连接池，可重复调用。
//
// 顺序是刻意的：先关读池，再 TRUNCATE checkpoint，最后关写池。
// 反过来的话活跃的读连接会让 checkpoint 拿不到独占锁，WAL 文件就留在了磁盘上。
func (d *DB) Close() error {
	d.once.Do(func() {
		var errs []error

		if d.read != nil && d.read != d.write {
			if err := d.read.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		if d.write != nil {
			// 尽力而为：失败不影响数据安全，下次 Open 会重放 WAL。
			if _, err := d.Checkpoint(CheckpointTruncate); err != nil && d.cfg.Debug {
				fmt.Printf("gormsqlite: 关闭前 checkpoint 失败（数据仍安全）: %v\n", err)
			}
			if err := d.write.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		d.err = errors.Join(errs...)
	})
	return d.err
}

// logConfig 打印实际生效的配置。PRAGMA 是回查出来的，不是照抄配置。
func (d *DB) logConfig() {
	why := ""
	switch {
	case d.auto:
		why = fmt.Sprintf("（GOMAXPROCS=%d，Auto 自动选择）", runtime.GOMAXPROCS(0))
	case isMemory(d.cfg.Path):
		why = "（内存库强制单连接）"
	}
	fmt.Printf("gormsqlite: path=%s mode=%s%s driver=mattn\n", d.cfg.Path, d.mode, why)
	fmt.Printf("  写池 MaxOpen=1  %s\n", snapshot(d.write))
	if d.mode == Split {
		fmt.Printf("  读池 MaxOpen=%d  %s\n", d.cfg.ReadConns, snapshot(d.read))
	} else {
		fmt.Printf("  读池 与写池共用同一条连接\n")
	}
}

func snapshot(db *sql.DB) string {
	var journal string
	var sync, busy, queryOnly int
	_ = db.QueryRow("PRAGMA journal_mode").Scan(&journal)
	_ = db.QueryRow("PRAGMA synchronous").Scan(&sync)
	_ = db.QueryRow("PRAGMA busy_timeout").Scan(&busy)
	_ = db.QueryRow("PRAGMA query_only").Scan(&queryOnly)
	names := map[int]string{0: "OFF", 1: "NORMAL", 2: "FULL", 3: "EXTRA"}
	s := names[sync]
	if s == "" {
		s = strconv.Itoa(sync)
	}
	out := fmt.Sprintf("journal=%s synchronous=%s busy_timeout=%d", journal, s, busy)
	if queryOnly != 0 {
		out += " query_only=1"
	}
	return out
}

// openPool 打开一个池并 Ping 一次。Ping 是必要的：它让连接真正建立，
// 从而在这一刻就暴露出文件打不开、PRAGMA 被拒之类的问题。
func openPool(cfg Config, maxConns int, readOnly bool) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dsn(cfg, readOnly))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxLifetime(0) // 本地文件，连接不该过期
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// dsn 拼 mattn 的连接串。注意语法是 _journal_mode=WAL 这种独立键，
// 写成 modernc 的 _pragma=journal_mode(WAL) 会被静默忽略。
func dsn(cfg Config, readOnly bool) string {
	q := map[string]string{
		"_journal_mode": "WAL",
		"_synchronous":  cfg.Synchronous,
		"_busy_timeout": strconv.FormatInt(cfg.BusyTimeout.Milliseconds(), 10),
		// 负数表示单位是 KB
		"_cache_size": strconv.Itoa(-cfg.CacheMB * 1024),
	}
	if readOnly {
		// 只读护栏：写操作由 SQLite 自己拒绝，不靠代码约定。
		q["_query_only"] = "1"
	} else {
		// BEGIN IMMEDIATE：事务开始即拿写锁，busy_timeout 才能生效。
		q["_txlock"] = "immediate"
	}
	for k, v := range cfg.ExtraDSN {
		q[k] = v // 同名覆盖内置值
	}

	// 键排序保证 DSN 可复现，便于排查
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString("file:")
	b.WriteString(escapePath(cfg.Path))
	for i, k := range keys {
		if i == 0 {
			b.WriteByte('?')
		} else {
			b.WriteByte('&')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(q[k])
	}
	return b.String()
}

// escapePath 处理会破坏 URI 解析的字符。必须先转义 % 本身，
// 否则后面生成的 %3F / %23 会被二次解码。
func escapePath(p string) string {
	p = strings.ReplaceAll(p, "\\", "/")
	p = strings.ReplaceAll(p, "%", "%25")
	p = strings.ReplaceAll(p, "?", "%3F")
	p = strings.ReplaceAll(p, "#", "%23")
	return p
}

func withDefaults(c Config) Config {
	if c.Synchronous == "" {
		c.Synchronous = "FULL"
	}
	if c.BusyTimeout <= 0 {
		c.BusyTimeout = defaultBusyTimeout
	}
	if c.CacheMB <= 0 {
		c.CacheMB = defaultCacheMB
	}
	if c.ReadConns <= 0 {
		n := runtime.GOMAXPROCS(0)
		if n > maxReadConns {
			n = maxReadConns
		}
		if n < 1 {
			n = 1
		}
		c.ReadConns = n
	}
	return c
}

func isMemory(path string) bool {
	return path == ":memory:" || strings.Contains(path, "mode=memory")
}
