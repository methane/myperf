package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB
var stmt *sql.Stmt

func attack(stop chan bool, result chan int64) {
	var count int64
	defer func() {
		result <- count
	}()
loop:
	for {
		select {
		case <-stop:
			log.Println("stop")
			break loop
		default:
			row := stmt.QueryRow()
			var n int
			err := row.Scan(&n)
			switch {
			case err != nil:
				log.Println(err)
			case n != 2:
				log.Println("Bad Result ", n)
			default:
				count++
			}
		}
	}
}

func run(concurrency, duration int) {
	stop := make(chan bool)
	result := make(chan int64)

	for i := 0; i < concurrency; i++ {
		go attack(stop, result)
	}
	<-time.After(time.Second * time.Duration(duration))
	log.Println("Stopping...")
	close(stop)

	var sum int64
	for i := 0; i < concurrency; i++ {
		sum += <-result
	}

	fmt.Println("Total queries: ", sum)
	fmt.Printf("Rate: %f [q/s]\n", float64(sum)/float64(duration))
}

func main() {
	var concurrency, duration int
	var dsn, query string
	flag.IntVar(&concurrency, "concurrency", 10, "Concurency")
	flag.IntVar(&duration, "duration", 10, "Duration [sec]")
	flag.StringVar(&dsn, "dsn", "", "DSN (see https://github.com/go-sql-driver/mysql#dsn-data-source-name)")
	flag.StringVar(&query, "query", "SELECT 1+1", "Query to send")
	flag.Parse()

	var err error
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxIdleConns(concurrency)
	stmt, err = db.Prepare("SELECT 1+1")
	if err != nil {
		log.Fatal(err)
	}
	run(concurrency, duration)
}
