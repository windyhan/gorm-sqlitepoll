package gormsqlite

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"gorm.io/gorm"
)

type Item struct {
	ID   int64 `gorm:"primaryKey"`
	Name string
}

func (Item) TableName() string { return "items" }

func openDB(t *testing.T, cfg Config) *DB {
	t.Helper()
	if cfg.Path == "" {
		cfg.Path = filepath.Join(t.TempDir(), "t.db")
	}
	d, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	if err := d.AutoMigrate(&Item{}); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	return d
}

// 单连接形态：读写共用一条连接，CRUD 和事务都要正常。
func TestSingleCRUD(t *testing.T) {
	d := openDB(t, Config{Mode: Single})

	w, r := d.Pools()
	if w != r {
		t.Fatal("Single 模式下读池必须是写池本身")
	}
	if got := d.Stats().Write.MaxOpenConnections; got != 1 {
		t.Fatalf("写池 MaxOpen=%d, want 1", got)
	}

	if err := d.Create(&Item{ID: 1, Name: "a"}).Error; err != nil {
		t.Fatalf("Create: %v", err)
	}
	var got Item
	if err := d.First(&got, 1).Error; err != nil {
		t.Fatalf("First: %v", err)
	}
	if got.Name != "a" {
		t.Fatalf("got %+v", got)
	}
	if err := d.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&Item{ID: 2, Name: "tx"}).Error; err != nil {
			return err
		}
		var n int64
		return tx.Model(&Item{}).Count(&n).Error
	}); err != nil {
		t.Fatalf("Transaction: %v", err)
	}
}

// 分离形态：写池 1、读池 N，读池上的写必须被 SQLite 拒绝。
func TestSplitRouting(t *testing.T) {
	d := openDB(t, Config{Mode: Split, ReadConns: 3})

	w, r := d.Pools()
	if w == r {
		t.Fatal("Split 模式下读池必须是独立的 *sql.DB")
	}
	st := d.Stats()
	if st.Write.MaxOpenConnections != 1 || st.Read.MaxOpenConnections != 3 {
		t.Fatalf("池大小 = 写 %d / 读 %d, want 1 / 3",
			st.Write.MaxOpenConnections, st.Read.MaxOpenConnections)
	}

	if err := d.Create(&Item{ID: 1, Name: "a"}).Error; err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := r.Exec("UPDATE items SET name = 'b' WHERE id = 1"); err == nil {
		t.Fatal("读池上的写操作本应失败，却成功了")
	} else if !strings.Contains(err.Error(), "readonly") {
		t.Fatalf("期望 readonly 错误，实际: %v", err)
	}

	// 事务内读己之写
	if err := d.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&Item{ID: 9, Name: "tx"}).Error; err != nil {
			return err
		}
		var got Item
		return tx.First(&got, 9).Error
	}); err != nil {
		t.Fatalf("事务内读不到刚写的行: %v", err)
	}
}

func TestAutoTopology(t *testing.T) {
	old := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(old)

	d1 := openDB(t, Config{Mode: Auto})
	if d1.EffectiveMode() != Single {
		t.Fatalf("1 核应选 Single，实际 %s", d1.EffectiveMode())
	}

	runtime.GOMAXPROCS(2)
	d2 := openDB(t, Config{Mode: Auto})
	if d2.EffectiveMode() != Split {
		t.Fatalf("2 核应选 Split，实际 %s", d2.EffectiveMode())
	}
	if got := d2.Config().ReadConns; got != 2 {
		t.Fatalf("2 核读连接数=%d, want 2", got)
	}
	if st := d2.Stats(); st.Write.MaxOpenConnections != 1 || st.Read.MaxOpenConnections != 2 {
		t.Fatalf("2 核池大小 = 写 %d / 读 %d, want 1 / 2",
			st.Write.MaxOpenConnections, st.Read.MaxOpenConnections)
	}
}

func TestCheckpointAndClose(t *testing.T) {
	d := openDB(t, Config{Mode: Split, ReadConns: 2})
	rows := make([]Item, 200)
	for i := range rows {
		rows[i] = Item{ID: int64(i + 1), Name: "n"}
	}
	if err := d.CreateInBatches(rows, 100).Error; err != nil {
		t.Fatalf("seed: %v", err)
	}
	if sz, _ := d.WALSize(); sz == 0 {
		t.Fatal("写入后 -wal 应大于 0")
	}

	res, err := d.Checkpoint(CheckpointTruncate)
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if res.Busy {
		t.Fatalf("没有活跃读事务，不该 busy: %+v", res)
	}
	if sz, _ := d.WALSize(); sz != 0 {
		t.Fatalf("TRUNCATE 后 -wal 应为 0，实际 %d", sz)
	}

	var cnt int64
	if err := d.Model(&Item{}).Count(&cnt).Error; err != nil {
		t.Fatalf("Count: %v", err)
	}
	if cnt != 200 {
		t.Fatalf("行数=%d, want 200", cnt)
	}

	if _, err := d.Checkpoint(CheckpointMode("X; DROP TABLE items")); err == nil {
		t.Fatal("非白名单模式应被拒绝")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("重复 Close 应无错: %v", err)
	}
}
