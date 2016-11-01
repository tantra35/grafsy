package main

import (
	"strconv"
	"os"
	"strings"
	"time"
	"log"
)

type pair struct {
	a int
	b int
}

type Monitoring struct {
	conf Config
	got Source
	saved int
	sent int
	dropped int
	invalid int
	lg *log.Logger
	chM chan string
	ch chan pair
}

type Source struct {
	net int
	dir int
	retry int
}

const monitorMetrics  = 7
const (
	DROP = 0
	SEND = 1
	SAVED = 2
	INVALID = 3
	GOTRETRY = 4
	GOTNET = 5
	GOTDIR = 6
	TIMEOUT = 100
)

func NewMonitoring(conf Config, lg *log.Logger, chM chan string) *Monitoring {
	return &Monitoring{conf, Source{}, 0, 0, 0, 0, lg, chM, make(chan pair)}
}

func (m *Monitoring) generateOwnMonitoring() []string {

	now := strconv.FormatInt(time.Now().Unix(),10)
	hostname,_ := os.Hostname()
	path := strings.Replace(m.conf.MonitoringPath, "HOSTNAME", strings.Replace(hostname, ".", "_", -1), -1) + ".grafsy."

	// If you add a new one - please increase monitorMetrics
	monitor_slice := []string{
		path + "got.net " + strconv.Itoa(m.got.net) + " " + now,
		path + "got.dir " + strconv.Itoa(m.got.dir) + " " + now,
		path + "got.retry " + strconv.Itoa(m.got.retry) + " " + now,
		path + "saved " + strconv.Itoa(m.saved) + " " + now,
		path + "sent " + strconv.Itoa(m.sent) + " " + now,
		path + "dropped " + strconv.Itoa(m.dropped) + " " + now,
		path + "invalid " + strconv.Itoa(m.invalid) + " " + now,
	}

	return monitor_slice
}

func (m *Monitoring) countDroped() {
	m.ch <- pair{DROP, 1}
}

func (m *Monitoring) countSend() {
	m.ch <- pair{SEND, 1}
}

func (m *Monitoring) countSaved() {
	m.ch <- pair{SAVED, 1}
}

func (m *Monitoring) countInvalid() {
	m.ch <- pair{INVALID, 1}
}

func (m *Monitoring) countGotRetry(count int) {
	m.ch <- pair{GOTRETRY, count}
}

func (m *Monitoring) countGotNet() {
	m.ch <- pair{GOTNET, 1}
}

func (m *Monitoring) countGotDir(count int) {
	m.ch <- pair{GOTDIR, count}
}

func (m *Monitoring) clean(){
	m.saved = 0
	m.sent = 0
	m.dropped = 0
	m.invalid = 0
	m.got = Source{0,0,0}
}

func (m *Monitoring) runMonitoring() {
	go func(ch chan pair){
		for {
			time.Sleep(time.Duration(60) * time.Second)
			ch <- pair{TIMEOUT, 0}
		}
	} (m.ch)

	for {
		item := <- m.ch

		switch item.a {
			case DROP:
				m.dropped += item.b

			case SEND:
				m.sent += item.b

			case SAVED:
				m.saved += item.b

			case INVALID:
				m.invalid += item.b

			case GOTRETRY:
				m.got.retry += item.b

			case GOTNET:
				m.got.net += item.b

			case GOTDIR:
				m.got.dir += item.b

			case TIMEOUT:
				if m.conf.MonitoringPath != "" {
					for _, metric := range m.generateOwnMonitoring() {
						select {
							case m.chM <- metric:
							default:
								m.lg.Printf("Too many metrics in the MON queue! This is very bad")
								m.dropped += 1
						}
					}
				}

				if m.dropped != 0 {
					m.lg.Printf("Too many metrics in the main buffer. Had to drop incommings")
				}

				m.clean()
		}
	}
}
