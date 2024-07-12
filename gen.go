package main

import (
	"bytes"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
)

func genOne(r *rand.Rand, buffer *bytes.Buffer, payloadBuffer []byte, payloadBufferEscaped []byte, numberBuffer []byte, from int64, count int64) {
	for i := from; i < from+count; i++ {
		// documentID
		numberBuffer = numberBuffer[:0]
		numberBuffer = strconv.AppendInt(numberBuffer, i, 10)
		buffer.Write(numberBuffer)
		buffer.WriteByte('\t')
		// payload
		n, err := r.Read(payloadBuffer)
		if err != nil || n < len(payloadBuffer) {
			panic(err)
		}
		payloadBufferEscapedLength := 0
		for j := 0; j < len(payloadBuffer); j++ {
			kk := payloadBuffer[j]
			if kk == '\\' || kk == '\t' || kk == '\n' {
				payloadBufferEscaped[payloadBufferEscapedLength] = '\\'
				payloadBufferEscaped[payloadBufferEscapedLength+1] = kk
				payloadBufferEscapedLength += 2
			} else {
				payloadBufferEscaped[payloadBufferEscapedLength] = kk
				payloadBufferEscapedLength++
			}
		}
		buffer.Write(payloadBufferEscaped[:payloadBufferEscapedLength])
		buffer.WriteByte('\n')
	}
}

func genMany(bufferRecycleChannel chan *bytes.Buffer, numCores int, payloadSize int64, batchSize int64, numRows int64) (<-chan *bytes.Buffer, *atomic.Int64) {
	wg := sync.WaitGroup{}
	remaining := &atomic.Int64{}
	remaining.Store(numRows)
	bufferReadyChannel := make(chan *bytes.Buffer, len(bufferRecycleChannel))
	for i := 0; i < numCores; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(i)))
			numberBuffer := make([]byte, 0, 20)
			payloadBuffer := make([]byte, payloadSize)
			payloadBufferEscaped := make([]byte, payloadSize*2)
			for newRemaining := remaining.Add(-batchSize); newRemaining > -batchSize; newRemaining = remaining.Add(-batchSize) {
				buffer := <-bufferRecycleChannel
				buffer.Reset()

				from := newRemaining
				count := batchSize

				if newRemaining < 0 {
					from = 0
					count = batchSize + newRemaining
				}

				genOne(r, buffer, payloadBuffer, payloadBufferEscaped, numberBuffer, from, count)
				bufferReadyChannel <- buffer
			}
		}()
	}
	go func() {
		wg.Wait()
		close(bufferReadyChannel)
	}()
	return bufferReadyChannel, remaining
}

func genAll(numBuffers int, bufferSizeBytes int64, payloadSize int64, numRows int64, numCores int) (<-chan *bytes.Buffer, chan<- *bytes.Buffer, *atomic.Int64) {
	bufferRecycleChannel := make(chan *bytes.Buffer, numBuffers)
	for i := 0; i < numBuffers; i++ {
		bufferRecycleChannel <- new(bytes.Buffer)
	}
	batchSize := bufferSizeBytes / (2*int64(len(strconv.FormatInt(math.MaxInt64, 10))) + payloadSize + 3)
	bufferReadyChannel, remaining := genMany(bufferRecycleChannel, numCores, payloadSize, batchSize, numRows)
	return bufferReadyChannel, bufferRecycleChannel, remaining
}
