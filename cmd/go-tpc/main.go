package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pingcap/go-tpc/pkg/util"
	"github.com/spf13/cobra"

	// mysql package
	_ "github.com/go-sql-driver/mysql"
	// pg
	_ "github.com/lib/pq"
)

var (
	dbName         string
	host           string
	port           int
	statusPort     int
	user           string
	password       string
	threads        int
	acThreads      int
	driver         string
	totalTime      time.Duration
	totalCount     int
	dropData       bool
	ignoreError    bool
	outputInterval time.Duration
	isolationLevel int
	silence        bool
	pprofAddr      string
	metricsAddr    string
	maxProcs       int
	connParams     string
	outputStyle    string

	globalDB  *sql.DB
	globalCtx context.Context
)

const (
	createDBDDL = "CREATE DATABASE "
	mysqlDriver = "mysql"
	pgDriver    = "postgres"
)

func closeDB() {
	if globalDB != nil {
		globalDB.Close()
	}
	globalDB = nil
}

func buildDSN(tmp bool) string {
	switch driver {
	case mysqlDriver:
		if tmp {
			return fmt.Sprintf("%s:%s@tcp(%s:%d)/", user, password, host, port)
		}
		// allow multiple statements in one query to allow q15 on the TPC-H
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?multiStatements=true", user, password, host, port, dbName)
		if len(connParams) > 0 {
			dsn = dsn + "&" + connParams
		}
		return dsn
	case pgDriver:
		if tmp {
			return fmt.Sprintf("postgres://%s:%s@%s:%d/?%s", user, password, host, port, connParams)
		}
		dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", user, password, host, port, dbName)
		if len(connParams) > 0 {
			dsn = dsn + "?" + connParams
		}
		return dsn
	default:
		panic(fmt.Errorf("unknown driver: %q", driver))
	}
}

func isDBNotExist(err error) bool {
	if err == nil {
		return false
	}
	switch driver {
	case mysqlDriver:
		return strings.Contains(err.Error(), "Unknown database")
	case pgDriver:
		msg := err.Error()
		return strings.HasPrefix(msg, "pq: database") && strings.HasSuffix(msg, "does not exist")
	}
	return false
}

func openDB() {
	var (
		tmpDB *sql.DB
		err   error
	)
	globalDB, err = sql.Open(driver, buildDSN(false))
	if err != nil {
		panic(err)
	}
	if err := globalDB.Ping(); err != nil {
		if isDBNotExist(err) {
			tmpDB, _ = sql.Open(driver, buildDSN(true))
			defer tmpDB.Close()
			if _, err := tmpDB.Exec(createDBDDL + dbName); err != nil {
				panic(fmt.Errorf("failed to create database, err %v\n", err))
			}
		} else {
			globalDB = nil
		}
	} else {
		globalDB.SetMaxIdleConns(threads + acThreads + 1)
	}
}

func main() {
	var rootCmd = &cobra.Command{
		Use:   "go-tpc",
		Short: "Benchmark database with different workloads",
	}
	rootCmd.PersistentFlags().IntVar(&maxProcs, "max-procs", 0, "runtime.GOMAXPROCS")
	rootCmd.PersistentFlags().StringVar(&pprofAddr, "pprof", "", "Address of pprof endpoint")
	rootCmd.PersistentFlags().StringVar(&metricsAddr, "metrics-addr", "", "Address of metrics endpoint")
	rootCmd.PersistentFlags().StringVarP(&dbName, "db", "D", "test", "Database name")
	rootCmd.PersistentFlags().StringVarP(&host, "host", "H", "127.0.0.1", "Database host")
	rootCmd.PersistentFlags().StringVarP(&user, "user", "U", "root", "Database user")
	rootCmd.PersistentFlags().StringVarP(&password, "password", "p", "", "Database password")
	rootCmd.PersistentFlags().IntVarP(&port, "port", "P", 4000, "Database port")
	rootCmd.PersistentFlags().IntVarP(&statusPort, "statusPort", "S", 10080, "Database status port")
	rootCmd.PersistentFlags().IntVarP(&threads, "threads", "T", 1, "Thread concurrency")
	rootCmd.PersistentFlags().IntVarP(&acThreads, "acThreads", "t", 1, "OLAP client concurrency, only for CH-benCHmark")
	rootCmd.PersistentFlags().StringVarP(&driver, "driver", "d", mysqlDriver, "Database driver: mysql, postgres")
	rootCmd.PersistentFlags().DurationVar(&totalTime, "time", 1<<63-1, "Total execution time")
	rootCmd.PersistentFlags().IntVar(&totalCount, "count", 0, "Total execution count, 0 means infinite")
	rootCmd.PersistentFlags().BoolVar(&dropData, "dropdata", false, "Cleanup data before prepare")
	rootCmd.PersistentFlags().BoolVar(&ignoreError, "ignore-error", false, "Ignore error when running workload")
	rootCmd.PersistentFlags().BoolVar(&silence, "silence", false, "Don't print error when running workload")
	rootCmd.PersistentFlags().DurationVar(&outputInterval, "interval", 10*time.Second, "Output interval time")
	rootCmd.PersistentFlags().IntVar(&isolationLevel, "isolation", 0, `Isolation Level 0: Default, 1: ReadUncommitted,
2: ReadCommitted, 3: WriteCommitted, 4: RepeatableRead,
5: Snapshot, 6: Serializable, 7: Linerizable`)
	rootCmd.PersistentFlags().StringVar(&connParams, "conn-params", "", "session variables, e.g. for TiDB --conn-params tidb_isolation_read_engines='tiflash', For PostgreSQL: --conn-params sslmode=disable")
	rootCmd.PersistentFlags().StringVar(&outputStyle, "output", util.OutputStylePlain, "output style, valid values can be { plain | table | json }")

	cobra.EnablePrefixMatching = true

	registerVersionInfo(rootCmd)
	registerTpcc(rootCmd)
	registerTpch(rootCmd)
	registerCHBenchmark(rootCmd)
	registerRawsql(rootCmd)

	var cancel context.CancelFunc
	globalCtx, cancel = context.WithCancel(context.Background())

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	closeDone := make(chan struct{}, 1)
	go func() {
		sig := <-sc
		fmt.Printf("\nGot signal [%v] to exit.\n", sig)
		cancel()

		select {
		case <-sc:
			// send signal again, return directly
			fmt.Printf("\nGot signal [%v] again to exit.\n", sig)
			os.Exit(1)
		case <-time.After(10 * time.Second):
			fmt.Print("\nWait 10s for closed, force exit\n")
			os.Exit(1)
		case <-closeDone:
			return
		}
	}()

	rootCmd.Execute()

	cancel()
}
