package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"io"
	"math"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	minBufferSizeBytes         = 16 * 1024 * 1024
	maxDirectBufferSizeBytes   = 256 * 1024 * 1024
	maxPipelineBufferSizeBytes = 1024 * 1024 * 1024
	maxTotalBufferSize         = 255
	maxPayloadSize             = 1024 * 1024
)

func usage() {
	fmt.Println("megapush ram_gib host port user password database table num_rows payload_size [s3_bucket]")
	os.Exit(1)
}

func main() {
	if len(os.Args) != 10 && len(os.Args) != 11 {
		usage()
	}
	args, err := parseArgs()
	if err != nil {
		fmt.Println(err)
		usage()
	}
	err = mainInner(args)
	if err != nil {
		fmt.Println(err)
	}
}

func mainInner(args *Args) error {
	db, err := connect(args.user, args.password, args.host, args.port, args.database)
	if err != nil {
		return err
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}(db)

	err = execDropAndCreateTable(db, args.table)
	if err != nil {
		return err
	}

	bufferReadyChannel, bufferRecycleChannel, remaining := genAll(args.numBuffers, args.bufferSize, args.payloadSize, args.numRows, runtime.NumCPU())
	waitForInitialBuffersWithProgress(bufferReadyChannel, args.numBuffers, remaining)

	var start time.Time
	if args.s3Bucket != "" {
		fmt.Println("Connecting to S3 and ensuring empty bucket")
		ctx := context.Background()
		s3, err := connectS3(ctx, args.s3Bucket)
		if err != nil {
			return err
		}
		fmt.Println("Start time: ", time.Now().Format("2006-01-02 15:04:05"))
		start = time.Now()
		err = mainPipeline(ctx, args, db, s3, bufferReadyChannel, bufferRecycleChannel, start)
		if err != nil {
			return err
		}
	} else {
		fmt.Println("Using LOAD DATA LOCAL INFILE method")
		fmt.Println("Start time: ", time.Now().Format("2006-01-02 15:04:05"))
		start = time.Now()
		numConcurrentUploads := runtime.NumCPU() * 2
		startLoadDataLocalInfileAll(bufferReadyChannel, bufferRecycleChannel, db, numConcurrentUploads, args.table)
		err = waitForLoadWithProgress(db, args.table, args.numRows, args.payloadSize, start)
		if err != nil {
			return err
		}
	}
	fmt.Println("End time: ", time.Now().Format("2006-01-02 15:04:05"))
	elapsed := time.Since(start)
	fmt.Println("Elapsed time: ", elapsed)
	fmt.Println("done")

	return nil
}

func mainPipeline(ctx context.Context, args *Args, db *sql.DB, s3 *s3Connection, bufferReadyChannel <-chan *bytes.Buffer, bufferRecycleChannel chan<- *bytes.Buffer, start time.Time) error {
	fmt.Println("Starting upload to S3")
	numConcurrentUploads := runtime.NumCPU() * 2
	err := startUploadToS3All(ctx, bufferReadyChannel, bufferRecycleChannel, s3, numConcurrentUploads)
	if err != nil {
		return err
	}
	fmt.Println("Creating pipeline")
	err = execCreatePipeline(db, args.table, args.s3Bucket, os.ExpandEnv("$AWS_REGION"), os.ExpandEnv("$AWS_ACCESS_KEY_ID"), os.ExpandEnv("$AWS_SECRET_ACCESS_KEY"), os.ExpandEnv("$AWS_SESSION_TOKEN"))
	if err != nil {
		return err
	}
	fmt.Println("Starting pipeline")
	err = execStartPipeline(db, args.table)
	if err != nil {
		return err
	}
	wg := sync.WaitGroup{}
	go func() {
		defer wg.Done()
		err := deleteCompletedBatchesUntilNoneLeftAndStopped(ctx, db, args.database, args.table, s3)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}()
	err = waitForLoadWithProgress(db, args.table, args.numRows, args.payloadSize, start)
	if err != nil {
		return err
	}
	fmt.Println("Stopping pipeline")
	err = execStopPipeline(db, args.table)
	if err != nil {
		return err
	}
	fmt.Println("Dropping pipeline")
	err = execDropPipeline(db, args.table)
	if err != nil {
		return err
	}
	fmt.Println("Waiting for cleanup of bucket")
	wg.Wait()
	err = s3.deleteAll(ctx)
	if err != nil {
		return err
	}
	return nil
}

func connect(user string, password string, host string, port int, database string) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?tls=skip-verify", user, password, host, port, database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func waitForInitialBuffersWithProgress(bufferReadyChannel <-chan *bytes.Buffer, numBuffers int, remaining *atomic.Int64) {
	fmt.Println("Generating test data locally (filling buffers)")
	for len(bufferReadyChannel) < numBuffers && remaining.Load() > 0 {
		time.Sleep(500 * time.Millisecond)
		fmt.Printf("\r%d/%d", len(bufferReadyChannel), numBuffers)
	}
	fmt.Printf("\r%d/%d", numBuffers, numBuffers)
	fmt.Println()
}

func waitForLoadWithProgress(db *sql.DB, table string, numRows int64, payloadSize int64, start time.Time) error {
	totalBytesToLoad := numRows * payloadSize
	suffix := "MiB"
	divisor := int64(1024 * 1024)
	if totalBytesToLoad >= 10*1024*1024*1024 {
		suffix = "GiB"
		divisor = int64(1024 * 1024 * 1024)
	}
	for {
		countNow, err := queryCount(db, table)
		if err != nil {
			return err
		}
		loadedNow := countNow * payloadSize
		fmt.Printf("\r%4d / %4d %s %s", loadedNow/divisor, totalBytesToLoad/divisor, suffix, time.Since(start).Round(time.Second))
		if countNow == numRows {
			fmt.Printf("\r%4d / %4d %s %s", totalBytesToLoad/divisor, totalBytesToLoad/divisor, suffix, time.Since(start).Round(time.Second))
			break
		}
		time.Sleep(1 * time.Second)
	}
	fmt.Println()
	return nil
}

func startLoadDataLocalInfileAll(bufferReadyChannel <-chan *bytes.Buffer, bufferRecycleChannel chan<- *bytes.Buffer, db *sql.DB, numConnections int, table string) {
	for i := 0; i < numConnections; i++ {
		go func() {
			readerName := strconv.FormatInt(int64(i), 10)
			for {
				buffer, ok := <-bufferReadyChannel
				if !ok {
					return
				}
				reader := bytes.NewReader(buffer.Bytes())
				mysql.RegisterReaderHandler(readerName, func() io.Reader { return reader })
				_, err := execLoadDataLocalInfile(db, readerName, table)
				if err != nil {
					if err != nil {
						fmt.Println(err)
					}
					os.Exit(1)
				}
				mysql.DeregisterReaderHandler(readerName)
				bufferRecycleChannel <- buffer
			}
		}()
	}
}

func startUploadToS3All(ctx context.Context, bufferReadyChannel <-chan *bytes.Buffer, bufferRecycleChannel chan<- *bytes.Buffer, s3 *s3Connection, numConnections int) error {
	err := s3.head(ctx)
	if err != nil {
		return err
	}
	err = s3.deleteAll(ctx)
	if err != nil {
		return err
	}
	bucketIndex := atomic.Int64{}
	for i := 0; i < numConnections; i++ {
		go func() {
			for {
				buffer, ok := <-bufferReadyChannel
				if !ok {
					return
				}
				index := bucketIndex.Add(1)
				err := s3.put(ctx, index, buffer.Bytes())
				if err != nil {
					if err != nil {
						fmt.Println(err)
					}
					os.Exit(1)
				}
				bufferRecycleChannel <- buffer
			}
		}()
	}
	return nil
}

type Args struct {
	host        string
	port        int
	user        string
	password    string
	database    string
	table       string
	numRows     int64
	payloadSize int64
	s3Bucket    string
	numBuffers  int
	bufferSize  int64
}

func parseArgs() (*Args, error) {
	totalBufferRaw := os.Args[1]
	host := os.Args[2]
	portRaw := os.Args[3]
	user := os.Args[4]
	password := os.Args[5]
	database := os.Args[6]
	table := os.Args[7]
	numRowsRaw := os.Args[8]
	randomPayloadSizeRaw := os.Args[9]
	s3Bucket := ""

	if len(os.Args) == 11 {
		s3Bucket = os.Args[10]
	}

	parsedTotalBuffer, err := strconv.ParseInt(totalBufferRaw, 10, 16)
	if err != nil {
		return nil, err
	}
	if parsedTotalBuffer <= 0 || parsedTotalBuffer > maxTotalBufferSize {
		return nil, fmt.Errorf("buffer_gib must be > 0 and <= " + strconv.FormatUint(maxTotalBufferSize, 10))
	}

	parsedPort, err := strconv.ParseUint(portRaw, 10, 16)
	if err != nil {
		return nil, err
	}

	parsedNumRows, err := strconv.ParseUint(numRowsRaw, 10, 64)
	if err != nil {
		return nil, err
	}
	if parsedNumRows == 0 || parsedNumRows > math.MaxInt64 {
		return nil, fmt.Errorf("num_rows must be > 0 and <= 9223372036854775807")
	}
	numRows := int64(parsedNumRows)

	parsedPayloadSize, err := strconv.ParseUint(randomPayloadSizeRaw, 10, 16)
	if err != nil {
		return nil, err
	}
	if parsedPayloadSize == 0 || parsedPayloadSize > maxPayloadSize {
		return nil, fmt.Errorf("random_payload_size must be > 0 and <= " + strconv.FormatUint(maxPayloadSize, 10))
	}
	payloadSize := int64(parsedPayloadSize)

	targetNumberOfBuffers := int64(runtime.NumCPU() * 4)
	targetBufferSize := (payloadSize * numRows) / targetNumberOfBuffers
	maxBufferSize := int64(maxDirectBufferSizeBytes)
	if s3Bucket != "" {
		maxBufferSize = maxPipelineBufferSizeBytes
	}
	bufferSize := min(targetBufferSize, maxBufferSize)
	bufferSize = max(bufferSize, minBufferSizeBytes)
	targetNumberOfBuffers = min(targetNumberOfBuffers, (parsedTotalBuffer*1024*1024*1024)/bufferSize)

	if s3Bucket != "" {
		if os.ExpandEnv("$AWS_REGION") == "" || os.ExpandEnv("$AWS_ACCESS_KEY_ID") == "" || os.ExpandEnv("$AWS_SECRET_ACCESS_KEY") == "" || os.ExpandEnv("$AWS_SESSION_TOKEN") == "" {
			return nil, fmt.Errorf("AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_SESSION_TOKEN must be set to use the S3 pipeline")
		}
	}

	return &Args{
		host:        host,
		port:        int(parsedPort),
		user:        user,
		password:    password,
		database:    database,
		table:       table,
		numRows:     numRows,
		payloadSize: payloadSize,
		s3Bucket:    s3Bucket,
		numBuffers:  int(targetNumberOfBuffers),
		bufferSize:  bufferSize,
	}, nil
}
