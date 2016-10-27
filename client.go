package main

import (
	"log"
	"net"
	"os"
	"time"
	"strings"
)

const METRIBUFFERSIZE = 20

type Client struct {
	conf         Config
	lc           LocalConfig
	mon          *Monitoring
	graphiteAddr net.TCPAddr
	lg           log.Logger
	ch           chan string
	chM          chan string
	metricsBuffer [METRIBUFFERSIZE]string
	metricsBufferLength int
}

func NewClient(conf Config, lc LocalConfig, mon *Monitoring, graphiteAddr net.TCPAddr, lg log.Logger, ch chan string, chM chan string) *Client {
	return &Client{conf, lc, mon, graphiteAddr, lg, ch, chM, [METRIBUFFERSIZE]string{}, 0}
}

/*
 Function saves []string to file. We need it cause it make a lot of IO to save and check size of file
 After every single metric
*/
func (c *Client) saveSliceToRetry(metrics []string) {
	f, err := os.OpenFile(c.conf.RetryFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		c.lg.Println(err.Error())
		return
	}

	for _, metric := range metrics {
		f.WriteString(metric + "\n")
		c.mon.saved++
	}

	f.Close()
	c.removeOldDataFromRetryFile()
}

func (c *Client) saveChannelToRetry(ch chan string, size int) {
	f, err := os.OpenFile(c.conf.RetryFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		c.lg.Println(err.Error())
		return
	}

	for i := 0; i < size; i++ {
		f.WriteString(<-ch + "\n")
		c.mon.saved++
	}

	f.Close()
	c.removeOldDataFromRetryFile()
}

/*
	Function is cleaning up retry-file
	wholeFile is sorted to have newest metrics on the beginning
	So we need to keep newest metrics
*/
func (c *Client) removeOldDataFromRetryFile() {
	currentLinesInFile := getSizeInLinesFromFile(c.conf.RetryFile)
	if currentLinesInFile > c.lc.fileMetricSize {
		c.lg.Printf("I can not save to %s more, than %d. I will have to drop the rest (%d)",
			c.conf.RetryFile, c.lc.fileMetricSize, currentLinesInFile-c.lc.fileMetricSize)
		// We save first c.lc.fileMetricSize of metrics (newest)
		wholeFile := readMetricsFromFile(c.conf.RetryFile)[currentLinesInFile - c.lc.fileMetricSize:]

		f, err := os.OpenFile(c.conf.RetryFile, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			c.lg.Println(err.Error())
		}

		defer f.Close()

		for _, metric := range wholeFile {
			f.WriteString(metric + "\n")
		}
	}
}

func (c *Client) metricsBufferSendToGraphite(conn net.Conn) error {
	_, err := conn.Write([]byte(strings.Join(c.metricsBuffer[: c.metricsBufferLength], "\n") + "\n"))

	if err != nil {
		c.saveSliceToRetry(c.metricsBuffer[: c.metricsBufferLength])
	}

	c.metricsBufferLength = 0

	return err
}

func (c *Client) tryToSendToGraphite(metric string, conn net.Conn) error {
	c.metricsBuffer[c.metricsBufferLength] = metric
	c.metricsBufferLength++

	if c.metricsBufferLength >= METRIBUFFERSIZE {
		metricsBufferLengthCached := c.metricsBufferLength
		err := c.metricsBufferSendToGraphite(conn)

		if err != nil {
			c.lg.Println("Write to server failed:", err.Error())
			return err
		}

		c.mon.sent += metricsBufferLengthCached
	}

	return nil
}

/*
	Sending data to graphite:
	1) Metrics from monitor queue
	2) Metrics from main quere
	3) Retry file
*/
func (c *Client) runClientOneStep() {
	// Try to dial to Graphite server. If ClientSendInterval is 10 seconds - dial should be no longer than 1 second
	conn, err := net.DialTimeout("tcp", c.graphiteAddr.String(), time.Duration(c.conf.ConnectTimeout)*time.Second)

	if err != nil {
		c.lg.Println("Can not connect to graphite server: ", err.Error())
		c.saveChannelToRetry(c.chM, len(c.chM))
		c.saveChannelToRetry(c.ch, len(c.ch))
		return
	}

	defer conn.Close()

	// We set dead line for connection to write. It should be the rest of we have for client interval
	err = conn.SetWriteDeadline(time.Now().Add(time.Duration(c.conf.ClientSendInterval - c.conf.ConnectTimeout - 1)*time.Second))
	if err != nil {
		c.lg.Println("Can not set deadline for connection: ", err.Error())
		c.saveChannelToRetry(c.chM, len(c.chM))
		c.saveChannelToRetry(c.ch, len(c.ch))
		return
	}

	connectionFailed := false
	processedTotal := 0

	// We send retry file first, we have a risk to lose old data
	retryFileMetrics := readMetricsFromFile(c.conf.RetryFile)
	for numOfMetricFromFile, metricFromFile := range retryFileMetrics {
		if numOfMetricFromFile + 1 >= c.lc.mainBufferSize {
			c.lg.Printf("Can read only %d metrics from %s. Rest will be kept for the next iteration", numOfMetricFromFile + 1, c.conf.RetryFile)
			c.saveSliceToRetry(retryFileMetrics[numOfMetricFromFile:])
			break
		}

		err = c.tryToSendToGraphite(metricFromFile, conn)
		if err != nil {
			// If we failed to write a metric to graphite - something is wrong with connection
			c.saveSliceToRetry(retryFileMetrics[numOfMetricFromFile:])
			connectionFailed = true
			break
		}

		c.mon.got.retry++
		processedTotal++
	}

	if connectionFailed {
		c.saveChannelToRetry(c.chM, len(c.chM))
		c.saveChannelToRetry(c.ch, len(c.ch))
		return
	}

	// Monitoring. We read it always and we reserved space for it
	bufSize := len(c.chM)

	for i := 0; i < bufSize; i++ {
		err = c.tryToSendToGraphite(<-c.chM, conn)
		if err != nil {
			c.saveChannelToRetry(c.chM, bufSize-i)
			connectionFailed = true
			break
		}
	}

	if connectionFailed {
		c.saveChannelToRetry(c.ch, len(c.ch))
		return
	}

	/*
	 Main Buffer. We read it completely but send only part which fits in mainBufferSize
	 Rests we save
	*/
	bufSize = len(c.ch)
	for processedMainBuff := 0; processedMainBuff < bufSize; processedMainBuff++ {
		if processedTotal >= c.lc.mainBufferSize {
			/*
			 Save only data for the moment of run. Concurrent goroutines know no mercy
			 and they continue to write...
			*/
			c.saveChannelToRetry(c.ch, bufSize - processedMainBuff)
			break
		}

		err = c.tryToSendToGraphite(<-c.ch, conn)
		if err != nil {
			c.saveChannelToRetry(c.ch, bufSize-processedMainBuff)
			connectionFailed = true
			break
		}

		processedTotal++
	}

	if !connectionFailed {
		//Flush rest
		if c.metricsBufferLength > 0 {
			c.metricsBufferSendToGraphite(conn)
		}
	}
}

func (c *Client) runClient() {
	sup := Supervisor{c.conf.Supervisor}

	for ; ; time.Sleep(time.Duration(c.conf.ClientSendInterval) * time.Second) {
		sup.notify() // Notify watchdog about aliveness of Client routine
		c.runClientOneStep()
	}
}
