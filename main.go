package main

import (
	"database/sql"
	"flag"
	"log"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

func main() {
	// 1. 接收命令行参数
	accountStr := flag.String("account", "", "Comma-separated list of account names")
	interval := flag.Int("interval", 30, "Interval (in seconds) to scrape and push metrics")
	pushgateway := flag.String("pushgateway", "http://localhost:9091", "Pushgateway address")
	job := flag.String("job", "", "Job name for Pushgateway")
	metricsName := flag.String("metrics", "", "Metrics name to push")
	instance := flag.String("instance", "", "Instance label for the metric")
	dsn := flag.String("dsn", "", "PostgreSQL DSN, e.g. 'postgres://user:pass@host:port/dbname?sslmode=disable'")

	flag.Parse()

	// 2. 解析账号列表
	if *accountStr == "" {
		log.Fatalf("No account provided. Use --account=acc1,acc2,...")
	}
	accounts := strings.Split(*accountStr, ",")

	// 3. 连接数据库
	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// 简单测试一下连接
	if err := db.Ping(); err != nil {
		log.Fatalf("DB ping error: %v", err)
	}

	// 4. 构造定时器，间隔 *interval 秒执行一次
	ticker := time.NewTicker(time.Duration(*interval) * time.Second)
	defer ticker.Stop()

	log.Printf("Start monitoring with interval=%ds, pushgateway=%s, job=%s, metrics=%s, instance=%s",
		*interval, *pushgateway, *job, *metricsName, *instance)

	// 5. 主循环：定时执行查询 & 推送
	for {
		select {
		case <-ticker.C:
			// 每次执行都重新构造一个 registry，收集所有账户的指标
			reg := prometheus.NewRegistry()

			for _, acc := range accounts {
				dailyReward, queryErr := queryDailyReward(db, acc)
				if queryErr != nil {
					log.Printf("queryDailyReward error for account=%s: %v", acc, queryErr)
					continue
				}

				// 为该 account 构造一个 Gauge，并设置值
				gauge := prometheus.NewGauge(prometheus.GaugeOpts{
					Name: *metricsName,
					ConstLabels: prometheus.Labels{
						"account": acc,
					},
				})
				gauge.Set(dailyReward)

				// 注册到 registry
				if err := reg.Register(gauge); err != nil {
					log.Printf("Register gauge error: %v", err)
				}
			}

			// 6. 通过 push.New(...) 将 registry 推送至 Pushgateway
			err = push.New(*pushgateway, *job).
				Grouping("instance", *instance).
				Gatherer(reg).
				Push()
			if err != nil {
				log.Printf("Could not push to Pushgateway: %v", err)
			} else {
				log.Println("Metrics pushed successfully.")
			}
		}
	}
}

// queryDailyReward 查询单个 account 的今日收益
func queryDailyReward(db *sql.DB, account string) (float64, error) {
	// 这里的 SQL 使用 AT TIME ZONE 'Asia/Shanghai'，并确保与“今天”对比也是在上海时区
	// 如有需要，可以根据自己逻辑进行修改
	sqlStmt := `
        SELECT COALESCE(SUM(reward)/1e6, 0)
        FROM epoch_distributor
        WHERE project = 'ALEO'
          AND miner_account_id IN (
              SELECT id FROM miner_account WHERE name = $1
          )
          AND DATE(epoch_time AT TIME ZONE 'Asia/Shanghai') = DATE(NOW() AT TIME ZONE 'Asia/Shanghai')
    `
	var dailyReward float64
	err := db.QueryRow(sqlStmt, account).Scan(&dailyReward)
	if err != nil {
		return 0, err
	}
	return dailyReward, nil
}
