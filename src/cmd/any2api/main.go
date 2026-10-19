// Command any2api 启动 Any2API 薄服务壳：HTTP 入口（OpenAI 面 + Anthropic 面）
// → 经转换内核 → 配置的上游。
//
// 用法：
//
//	any2api -config config.yaml
//
// 配置示例见仓库根 src/config.example.yaml。
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dwgx/any2api/service"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to YAML config file")
	flag.Parse()

	cfg, err := service.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("any2api: %v", err)
	}
	srv, err := newServer(cfg)
	if err != nil {
		log.Fatalf("any2api: %v", err)
	}
	httpSrv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("any2api: listening on %s (upstreams=%d, heartbeat=%ds)",
			cfg.Listen, len(cfg.Upstreams), cfg.HeartbeatSeconds)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("any2api: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Printf("any2api: shutting down")
	shutdown(httpSrv, srv, 5*time.Second)
}

// newServer 创建服务（启动期错误交回 main 统一 log.Fatalf——P1-1：store 打开/
// 迁移失败绝不静默降级；W59-1 §10-1 的显式 NewServer 接线）。
func newServer(cfg *service.Config) (*service.Server, error) {
	return service.NewServer(cfg)
}

// shutdown 停机收尾：HTTP 有界关停 → claude_web 清理（≤2s）→ 显式关库。
func shutdown(httpSrv *http.Server, srv *service.Server, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := httpSrv.Shutdown(ctx); err != nil {
		log.Printf("any2api: shutdown: %v", err)
	}
	// claude_web 会话清理收尾（W13-1）：取消在飞清理任务并有界等待，
	// 不阻塞超过 2s（超时只是 warn 级事实，见 service.ShutdownCleanup）。
	if !srv.ShutdownCleanup(2 * time.Second) {
		log.Printf("any2api: claude_web cleanup: timed out waiting for in-flight deletes")
	}
	closeStore(srv)
}

// closeStore 显式关闭持久化层（W59-1 §10-1：main 自己拥有 store 生命周期）。
// CloseStore 幂等——ShutdownCleanup 钩子内已关一次；失败只记日志，停机不中断。
func closeStore(srv *service.Server) {
	if err := srv.CloseStore(); err != nil {
		log.Printf("any2api: store close: %v", err)
	}
}
