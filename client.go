package main

import (
        "log"
        "net"
        "os"
        "time"
        "strings"
        "github.com/beeker1121/goque"
)

const METRIBUFFERSIZE = 200

type Client struct {
	conf         Config
	lc           LocalConfig
	mon          *Monitoring
	lg           *log.Logger
	ch           chan string
	chM          chan string
	goque        *goque.Queue
	goquech      chan *goque.Queue
	metricsBuffer [METRIBUFFERSIZE]string
	metricsBufferLength int
}

func NewClient(conf Config, lc LocalConfig, mon *Monitoring, lg *log.Logger, ch chan string, chM chan string) *Client {
        q, err := goque.OpenQueue(conf.RetryFile)
        if err != nil {
                lg.Println("Can't create retry queue:", err.Error())
                os.Exit(1)
        }

        qch := make(chan *goque.Queue, 1)

        return &Client{conf, lc, mon, lg, ch, chM, q, qch, [METRIBUFFERSIZE]string{}, 0}
}

/*
 Function saves []string to file. We need it cause it make a lot of IO to save and check size of file
 After every single metric
*/
func (c *Client) saveChannelToRetry(ch chan string, size int) {
        for i := 0; i < size; i++ {
                metric := <-ch

                if c.goque.Length() < uint64(c.lc.fileMetricSize) {
                        c.goque.EnqueueString(metric)
                        if len(c.goquech) == 0 { c.goquech <- c.goque }
                        c.mon.countSaved()
                } else {
                        c.mon.countDroped()
                }
        }
}

func (c *Client) metricsBufferSendToGraphite(conn net.Conn) error {
	_, err := conn.Write([]byte(strings.Join(c.metricsBuffer[: c.metricsBufferLength], "\n") + "\n"))

	if err != nil {
		metrics := c.metricsBuffer[: c.metricsBufferLength]

		for _, metric := range metrics {
			c.goque.EnqueueString(metric)
			if len(c.goquech) == 0 { c.goquech <- c.goque }
			c.mon.countSaved()
		}
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

		c.mon.countSentNum(metricsBufferLengthCached)
	}

	return nil
}

func (c *Client) establishConnectionToGraphite() (net.Conn, error) {
	// Try to dial to Graphite server. If ClientSendInterval is 10 seconds - dial should be no longer than 1 second
	conn, err := net.DialTimeout("tcp", c.conf.GraphiteAddr, time.Duration(c.conf.ConnectTimeout)*time.Second)
	if err != nil {
		c.lg.Println("Can not connect to graphite server: ", err.Error())
		return nil, err
	}

	// We set dead line for connection to write. It should be the rest of we have for client interval
	err = conn.SetWriteDeadline(time.Now().Add(time.Duration(c.conf.ClientSendInterval - c.conf.ConnectTimeout)*time.Second))
	if err != nil {
		conn.Close()
		c.lg.Println("Can not set deadline for connection: ", err.Error())

		return nil, err
	}

	return conn, nil
}

func (c *Client) runClientOneStepSendToGraphite(conn net.Conn, ch chan string, chCount int) (bool) {
	conn.SetWriteDeadline(time.Now().Add(time.Duration(c.conf.ClientSendInterval)*time.Second))

	for i := 0; i < chCount; i++ {
		err := c.tryToSendToGraphite(<-ch, conn)
		if err != nil {
			c.saveChannelToRetry(ch, chCount - i)
			return false
		}
	}

	return true
}

/*
        Sending data to graphite:
        1) Metrics from monitor queue
        2) Metrics from main quere
        3) Retry file
*/
func (c *Client) runClientOneStep(conn net.Conn) (bool) {
	l_chMCount := len(c.chM)
	l_chCount := len(c.ch)

	if conn == nil {
		c.saveChannelToRetry(c.chM, l_chMCount)
		c.saveChannelToRetry(c.ch, l_chCount)

		return false
	}

	// Monitoring. We read it always and we reserved space for it
	sendOK := c.runClientOneStepSendToGraphite(conn, c.chM, l_chMCount)

	if !sendOK {
		c.saveChannelToRetry(c.ch, l_chCount)
		return false
	}

	// Metrics
	sendOK = c.runClientOneStepSendToGraphite(conn, c.ch, l_chCount)

	if sendOK {
		conn.SetWriteDeadline(time.Now().Add(time.Duration(c.conf.ClientSendInterval + c.conf.ConnectTimeout) * time.Second))

		//Flush rest
		if c.metricsBufferLength > 0 {
			c.metricsBufferSendToGraphite(conn)
		}
	}

	return true
}

func (c *Client) sendRetry() {
        metricsBuffer := [METRIBUFFERSIZE]string{}
        metricsBufferLength := int(0)
        q := <- c.goquech
        sendAttempts := uint(0)

        var conn net.Conn
        var err error
        var sendAttemptCountdown uint

        for ;; {
                qlength := q.Length()
                c.lg.Println("Retry mertics queue length: ", qlength, ", metricsBufferLength: ", metricsBufferLength)

                if metricsBufferLength == 0 {
                        metricbufferfillloop:
                        for ; metricsBufferLength < METRIBUFFERSIZE; {
                                item, err := q.Dequeue()
                                if err != nil {
                                        select {
                                                case <- c.goquech:
                                                        //pass
                                                case <- time.After((time.Duration(c.conf.ClientSendInterval) * time.Second) / 2):
                                                        break metricbufferfillloop
                                        }
                                        continue
                                }

                                metricsBuffer[metricsBufferLength] = item.ToString()
                                metricsBufferLength++
                        }
                }

                // Если установлен обратный отсчет до следующей отправки, то доводим его до 0 и только потом пытаемся отправить снова
                if sendAttemptCountdown > 0 {
                        sendAttemptCountdown--
                        c.lg.Println("Countdown to next send attempt ", sendAttemptCountdown)
                        time.Sleep(time.Duration(c.conf.ClientSendInterval) * time.Second)
                        continue
                }

                //если в буфере что то есть нужно попытаться это отправить
                if metricsBufferLength != 0 {
                        if conn == nil {
                                conn, err = c.establishConnectionToGraphite()
                                if err != nil {
                                        sendAttempts++
                                        sendAttemptCountdown = sendAttempts
                                        continue
                                }

                                c.lg.Println("Establish new connection to graphite")
                        }

                        _, err := conn.Write([]byte(strings.Join(metricsBuffer[: metricsBufferLength], "\n") + "\n"))
                        if err != nil {
                                conn.Close()
                                conn = nil

                                c.lg.Println("Can't send metrics in retry: ", err.Error())

                                sendAttempts++
                                sendAttemptCountdown = sendAttempts
                                continue
                        }

                        // We set dead line for connection to write. It should be the rest of we have for client interval
                        err = conn.SetWriteDeadline(time.Now().Add(time.Duration(c.conf.ClientSendInterval) * time.Second))
                        if err != nil {
                                c.lg.Println("Can not set deadline for retry connection: ", err.Error())
                                conn.Close()
                                conn = nil
                        }

                        c.mon.countGotRetry(metricsBufferLength)
                        sendAttempts = 0
                        sendAttemptCountdown = 0
                        metricsBufferLength = 0
                } else {
                        if conn != nil {
                                c.lg.Println("Close graphite server connection in retry due unuse")
                                conn.Close()
                                conn = nil
                        }
                }
        }
}

func (c *Client) runClient() {
	sup := Supervisor{c.conf.Supervisor}

	//If in quue exitst eny items try to sedn them
	if c.goque.Length() > 0 {
		c.goquech <- c.goque
	}

	go c.sendRetry()
	var conn net.Conn
	var err error
	conn = nil

	for ; ; time.Sleep(time.Duration(c.conf.ClientSendInterval) * time.Second) {
		sup.notify() // Notify watchdog about aliveness of Client routine

		if conn == nil {
			conn, err = c.establishConnectionToGraphite()
			if err != nil {
				conn = nil
				c.lg.Println("Can't establish connection to carbon server")
			}
		}

		sendFailed := c.runClientOneStep(conn)
		if sendFailed {
			c.lg.Println("Required reestablish connection to carbonserver")
			conn.Close()
			conn = nil
		}
	}
}
