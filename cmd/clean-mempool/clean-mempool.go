package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"github.com/shruggr/1sat-indexer/lib"
)

var POSTGRES string

var ctx = context.Background()

func init() {
	wd, _ := os.Getwd()
	log.Println("CWD:", wd)
	godotenv.Load(fmt.Sprintf(`%s/../../.env`, wd))

	if POSTGRES == "" {
		POSTGRES = os.Getenv("POSTGRES_FULL")
	}
	var err error
	log.Println("POSTGRES:", POSTGRES)
	db, err := pgxpool.New(context.Background(), POSTGRES)
	if err != nil {
		log.Panic(err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDISDB"),
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	cache := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDISCACHE"),
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	err = lib.Initialize(db, rdb, cache)
	if err != nil {
		log.Panic(err)
	}
}

func main() {
	for {
		rowCount, cleaned, cleared := cleanupTxns()
		log.Printf("Rows:%d Updated:%d Cleared:%d\n", rowCount, cleaned, cleared)
		if rowCount == 0 {
			time.Sleep(time.Minute)
		}
	}
}

func cleanupTxns() (rowCount int, cleaned int, cleared int) {
	rows, err := lib.Db.Query(ctx, `SELECT txid FROM txns
		WHERE (height IS NULL OR height=0 OR idx=0) AND created < NOW() - interval '3h' AND rolledback IS NULL
		LIMIT 1000`)
	if err != nil {
		log.Panicln(err)
	}
	defer rows.Close()

	limiter := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for rows.Next() {
		var txid []byte
		err := rows.Scan(&txid)
		if err != nil {
			log.Panicln(err)
		}

		rowCount++
		wg.Add(1)
		limiter <- struct{}{}
		go func(txid []byte) {
			defer func() {
				<-limiter
				wg.Done()
			}()
			// A 404 is a real answer (tx is gone -> roll it back below). Any other error is
			// transient upstream trouble; panicking on it killed the whole process and pm2
			// restarted straight into the same failure -- 60 crashes in 9s during one 502 blip.
			txn, err := lib.JB.GetTransaction(ctx, hex.EncodeToString(txid))
			for attempt := 1; err != nil && !strings.HasPrefix(err.Error(), "server error: 404") && attempt < 5; attempt++ {
				log.Println("Validating", hex.EncodeToString(txid), err, "- retry", attempt)
				time.Sleep(time.Duration(attempt) * 2 * time.Second)
				txn, err = lib.JB.GetTransaction(ctx, hex.EncodeToString(txid))
			}
			if err != nil {
				log.Println("Validating", hex.EncodeToString(txid), err)
				if !strings.HasPrefix(err.Error(), "server error: 404") {
					// Leave this txid for the next pass rather than taking the process down.
					log.Println("Giving up on", hex.EncodeToString(txid), "this pass:", err)
					return
				}
			}
			if txn != nil && txn.BlockHeight > 0 {
				// log.Printf("Updating Txn: %x\n", txid)
				_, err = lib.Db.Exec(ctx, `UPDATE txos
					SET height=$2, idx=$3
					WHERE txid=$1`,
					txid,
					txn.BlockHeight,
					txn.BlockIndex,
				)
				if err != nil {
					log.Panicln(err)
				}
				_, err = lib.Db.Exec(ctx, `UPDATE txos
					SET spend_height=$2, spend_idx=$3
					WHERE spend=$1`,
					txid,
					txn.BlockHeight,
					txn.BlockIndex,
				)
				if err != nil {
					log.Panicln(err)
				}
				_, err := lib.Db.Exec(ctx, `UPDATE txns
					SET block_id=decode($2, 'hex'), height=$3, idx=$4
					WHERE txid=$1`,
					txid,
					txn.BlockHash,
					txn.BlockHeight,
					txn.BlockIndex,
				)
				if err != nil {
					log.Panicln(err)
				}
				cleaned++
			} else {
				log.Println("Rolling Back:", hex.EncodeToString(txid))
				result, err := lib.Db.Exec(ctx,
					"UPDATE txos SET spend='\\x' WHERE spend=$1",
					txid,
				)
				if err != nil {
					log.Panic(err)
				}
				log.Println("Spends Rolled back:", result.RowsAffected())

				result, err = lib.Db.Exec(ctx,
					"DELETE FROM txos WHERE txid=$1",
					txid,
				)
				if err != nil {
					log.Panic(err)
				}
				log.Println("Txos Rolled back:", result.RowsAffected())

				result, err = lib.Db.Exec(context.Background(),
					"UPDATE txns SET rolledback=CURRENT_TIMESTAMP WHERE txid=$1",
					txid,
				)
				if err != nil {
					log.Panic(err)
				}
				log.Println("Txns Rolled back:", result.RowsAffected())
				cleared++
			}
		}(txid)
	}
	wg.Wait()
	return
}
