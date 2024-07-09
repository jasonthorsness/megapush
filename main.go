package main

import (
	"bytes"
	"crypto/rand"
	"database/sql"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/schollz/progressbar/v3"
)

const (
	bufferSizeBytes    = 256 * 1024 * 1024
	maxPayloadSize     = 1024 * 1024
	maxTotalBufferSize = 255
)

type rowBatch struct {
	count  int64
	buffer *bytes.Buffer
}

func usage() {
	fmt.Println("Usage:")
	fmt.Println("megapush <buffer ram to use in GiB> host port user password database table <number of rows to insert> <bytes length of random payload in each row>")
	fmt.Println("Example:")
	fmt.Println(`megapush 8 svc-3482219c-a389-4079-b18b-d50662524e8a-shared-dml.aws-virginia-6.svc.singlestore.com 3333 jason-tmp SrO0P2kCjumi1ZtRTlzT2cCqrsvnOVFY jtdb pushed 10000000 64`)
}

func main() {
	err := mainInner()
	if err != nil {
		fmt.Println(err)
		usage()
		return
	}
}

func mainInner() error {
	if len(os.Args) != 10 {
		return fmt.Errorf("invalid number of arguments")
	}

	totalBufferRaw := os.Args[1]
	host := os.Args[2]
	portRaw := os.Args[3]
	user := os.Args[4]
	password := os.Args[5]
	database := os.Args[6]
	table := os.Args[7]
	numRowsRaw := os.Args[8]
	randomPayloadSizeRaw := os.Args[9]

	parsedTotalBuffer, err := strconv.ParseUint(totalBufferRaw, 10, 16)
	if err != nil {
		return err
	}
	if parsedTotalBuffer == 0 || parsedTotalBuffer > maxTotalBufferSize {
		return fmt.Errorf("buffer_gib must be > 0 and <= " + strconv.FormatUint(maxTotalBufferSize, 10))
	}
	numBuffers := (int64(parsedTotalBuffer) * 1024 * 1024 * 1024) / bufferSizeBytes

	parsedPort, err := strconv.ParseUint(portRaw, 10, 16)
	if err != nil {
		return err
	}

	parsedNumRows, err := strconv.ParseUint(numRowsRaw, 10, 64)
	if err != nil {
		return err
	}
	if parsedNumRows == 0 || parsedNumRows > math.MaxInt64 {
		return fmt.Errorf("num_rows must be > 0 and <= 9223372036854775807")
	}
	numRows := int64(parsedNumRows)

	parsedPayloadSize, err := strconv.ParseUint(randomPayloadSizeRaw, 10, 16)
	if err != nil {
		return err
	}
	if parsedPayloadSize == 0 || parsedPayloadSize > maxPayloadSize {
		return fmt.Errorf("random_payload_size must be > 0 and <= " + strconv.FormatUint(maxPayloadSize, 10))
	}
	payloadSize := int64(parsedPayloadSize)

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?tls=skip-verify", user, password, host, parsedPort, database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}(db)

	rows, err := db.Query("SELECT @@SINGLESTOREDB_VERSION")
	if err != nil {
		return err
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			panic(err)
		}
	}(rows)

	if !rows.Next() {
		return fmt.Errorf("no rows")
	}
	version := ""
	err = rows.Scan(&version)
	if err != nil {
		return err
	}
	fmt.Printf("Connected to SingleStore %s\n", version)

	query := `DROP TABLE IF EXISTS ` + table
	_, err = db.Exec(query)
	if err != nil {
		return err
	}
	query = `CREATE TABLE ` + table + `(
    documentID BIGINT NOT NULL,
    payload LONGBLOB NOT NULL,
    SORT KEY(documentID),
    SHARD KEY(documentID))`
	_, err = db.Exec(query)
	if err != nil {
		return err
	}

	remaining := atomic.Int64{}
	remaining.Store(numRows)
	loaded := atomic.Int64{}

	bufferReadyChannel := make(chan rowBatch, numBuffers)
	bufferRecycleChannel := make(chan *bytes.Buffer, numBuffers)
	wgGen := sync.WaitGroup{}

	for i := int64(0); i < numBuffers; i++ {
		bufferRecycleChannel <- new(bytes.Buffer)
	}

	batchSize := bufferSizeBytes / (2*int64(len(strconv.FormatInt(math.MaxInt64, 10))) + payloadSize + 3)

	// Generator
	for i := 0; i < runtime.NumCPU(); i++ {
		wgGen.Add(1)
		go func() {
			defer wgGen.Done()
			numberBuffer := make([]byte, 0, 20)
			payloadBuffer := make([]byte, payloadSize)
			for newRemaining := remaining.Add(-batchSize); newRemaining > -batchSize; newRemaining = remaining.Add(-batchSize) {
				buffer := <-bufferRecycleChannel
				buffer.Reset()

				from := newRemaining
				count := batchSize

				if newRemaining < 0 {
					from = 0
					count = batchSize + newRemaining
				}

				for i := from; i < from+count; i++ {
					n, err := rand.Read(payloadBuffer)
					if err != nil || n < len(payloadBuffer) {
						panic(err)
					}
					numberBuffer = numberBuffer[:0]
					numberBuffer = strconv.AppendInt(numberBuffer, i, 10)
					buffer.Write(numberBuffer)
					buffer.WriteByte('\t')
					for j := int64(0); j < payloadSize; j++ {
						if payloadBuffer[j] == '\\' || payloadBuffer[j] == '\t' || payloadBuffer[j] == '\n' {
							buffer.WriteByte('\\')
						}
						buffer.WriteByte(payloadBuffer[j])
					}
					buffer.WriteByte('\n')
				}

				bufferReadyChannel <- rowBatch{count, buffer}
			}
		}()
	}

	go func() {
		wgGen.Wait()
		close(bufferReadyChannel)
	}()

	fmt.Println("Start time: ", time.Now().Format("2006-01-02 15:04:05"))
	start := time.Now()

	// Pusher
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			readerName := strconv.FormatInt(int64(i), 10)
			for {
				batch, ok := <-bufferReadyChannel
				if !ok {
					return
				}
				mysql.RegisterReaderHandler(readerName, func() io.Reader { return bytes.NewReader(batch.buffer.Bytes()) })
				query := "LOAD DATA LOCAL INFILE 'Reader::" + readerName + "' INTO TABLE " + table
				_, err := db.Exec(query)
				if err != nil {
					fmt.Println(batch.count, batch.buffer.Len(), query)
					fmt.Println(err)
					errorFile := readerName + ".err.tsv"
					f, err := os.Create(errorFile)
					if err != nil {
						fmt.Println(err)
						os.Exit(1)
					}
					_, err = f.Write(batch.buffer.Bytes())
					if err != nil {
						fmt.Println(err)
					}
					err = f.Close()
					if err != nil {
						fmt.Println(err)
					}
					fmt.Println("Wrote to " + errorFile)
					os.Exit(1)
				}
				mysql.DeregisterReaderHandler(readerName)
				loaded.Add(payloadSize * batch.count)
				bufferRecycleChannel <- batch.buffer
			}
		}()
	}

	bar := progressbar.DefaultBytes(numRows * payloadSize)
	for {
		err := bar.Set64(loaded.Load())
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if remaining.Load() <= 0 && len(bufferRecycleChannel) == int(numBuffers) {
			err = bar.Finish()
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	fmt.Println("End time: ", time.Now().Format("2006-01-02 15:04:05"))
	elapsed := time.Since(start)
	fmt.Println("Elapsed time: ", elapsed)
	fmt.Println("done")
	return nil
}
