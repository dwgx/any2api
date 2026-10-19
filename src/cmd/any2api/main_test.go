package main

// main_test.go —— cmd/any2api 接线测试（W59-1 §10-1 / W60-SRC2 §⑧）：
// ① 启动期错误必须经 NewServer 返回给 main（不得走 New 的内部 log.Fatalf）；
// ② 停机收尾必须显式关闭持久化层（closeStore → Server.store = nil）。

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/dwgx/any2api/service"
)

// storeIsNil 只读反射 service.Server.store（未导出接口字段）：closeStore 是否
// 真的把持久化层置空——跨包无法直接读，IsNil 不需要 Interface()，合法。
func storeIsNil(s *service.Server) bool {
	f := reflect.ValueOf(s).Elem().FieldByName("store")
	return f.IsValid() && f.Kind() == reflect.Interface && f.IsNil()
}

// TestNewServerReturnsStoreError 启动期错误面：store 打不开时 newServer 返回
// error（main 的 NewServer 接线；service.New 会在进程内 Fatalf——测试进程能
// 跑到断言即证明未走那条路）。
func TestNewServerReturnsStoreError(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("write blocker: %v", err)
	}
	cfg := &service.Config{Store: &service.StoreSection{Enabled: true, Path: filepath.Join(blocker, "db.sqlite")}}
	srv, err := newServer(cfg)
	if err == nil {
		if srv != nil {
			_ = srv.CloseStore()
		}
		t.Fatal("store 路径不可用时应返回启动错误")
	}
	if srv != nil {
		t.Fatalf("错误路径不应返回 Server: %v", srv)
	}
}

// TestCloseStoreClosesPersistentLayer 显式 CloseStore 面：启用 store 后字段非
// nil；closeStore 后置空；再次调用幂等（不 panic、不报错）。
func TestCloseStoreClosesPersistentLayer(t *testing.T) {
	cfg := &service.Config{Store: &service.StoreSection{Enabled: true, Path: filepath.Join(t.TempDir(), "any2api.db")}}
	srv, err := newServer(cfg)
	if err != nil {
		t.Fatalf("newServer: %v", err)
	}
	if storeIsNil(srv) {
		t.Fatal("store.enabled=true 时 store 不应为 nil")
	}
	closeStore(srv)
	if !storeIsNil(srv) {
		t.Fatal("closeStore 后 store 应为 nil")
	}
	closeStore(srv) // 幂等
	if !storeIsNil(srv) {
		t.Fatal("重复 closeStore 后 store 应仍为 nil")
	}
}

// TestShutdownClosesHTTPAndStore 停机序列：HTTP 关停 + 清理钩子 + 显式关库，
// 全程有界返回。
func TestShutdownClosesHTTPAndStore(t *testing.T) {
	cfg := &service.Config{Store: &service.StoreSection{Enabled: true, Path: filepath.Join(t.TempDir(), "any2api.db")}}
	srv, err := newServer(cfg)
	if err != nil {
		t.Fatalf("newServer: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	done := make(chan struct{})
	go func() {
		shutdown(ts.Config, srv, time.Second)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown 未在 3s 内返回")
	}
	ts.Close()
	if !storeIsNil(srv) {
		t.Fatal("shutdown 后 store 应为 nil")
	}
}
