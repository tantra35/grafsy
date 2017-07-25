package main

import (
	"log"
	"os"
	"github.com/BurntSushi/toml"
	"gopkg.in/natefinch/lumberjack.v2"
	"fmt"
	"path/filepath"
	"flag"
)

type Config struct {
	Supervisor string
	ClientSendInterval int
	MetricsCollectInterval int
	MetricsPerSecond int
	GraphiteAddr string // Think about multiple servers
	ConnectTimeout int
	LocalBind string
	Log string
	MetricDir string
	RetryFile string
	SumPrefix string
	SumInterval int
	SumsPerSecond int
	AvgPrefix string
	AvgInterval int
	AvgsPerSecond int
	MonitoringPath string
	AllowedMetrics string
}

type LocalConfig struct {
	mainBufferSize int
	sumBufSize int
	avgBufSize int
	fileMetricSize int
}


func main() {
	var configFile string
	flag.StringVar(&configFile, "c", "/etc/grafsy/grafsy.toml", "Path to config file.")
	flag.Parse()

	var conf Config
	if _, err := toml.DecodeFile(configFile, &conf); err != nil {
		fmt.Println("Failed to parse config file", err.Error())
		os.Exit(1)
	}

	if _, err := os.Stat(filepath.Dir(conf.Log)); os.IsNotExist(err) {
		if os.MkdirAll(filepath.Dir(conf.Log), os.ModePerm) != nil {
			fmt.Println("Can not create logfile's dir " + filepath.Dir(conf.Log))
			os.Exit(1)
		}
	}

	f := &lumberjack.Logger{
		Filename:   conf.Log,
		MaxSize:    50, // megabytes
		MaxBackups: 7,
		MaxAge:     10, //days
	}

	lg := log.New(f, "", log.Ldate|log.Lmicroseconds|log.Lshortfile)

	/*
		Units - metric
	 */
	lc := LocalConfig{
		/*
			This is a main buffer
			Every ClientSendInterval you will send upto MetricsPerSecond per second
			It does not make any sense to have it too big cause metrics will be dropped during saving to file
			This buffer is ready to take MetricsPerSecond*ClientSendInterval. Which gives you the rule, than bigger interval you have or
			amount of metric in interval, than more metrics it can take in memory.
		*/
		conf.MetricsPerSecond*conf.ClientSendInterval,
		/*
			This is a sum buffer. I assume it make total sense to have maximum buf = SumsPerSecond*sumInterval.
			For example up to 60*60 sums per second
		*/
		conf.SumsPerSecond*conf.SumInterval,
		/*
			This is a avg buffer. I assume it make total sense to have maximum buf = SumsPerSecond*sumInterval.
			For example up to 60*60 sums per second
		 */
		conf.AvgsPerSecond*conf.AvgInterval,
		/*
			Retry file will take only 10 full buffers
		 */
		conf.MetricsPerSecond*conf.ClientSendInterval*100}

	/*
		Check if directories for temporary files exist
		This is especially important when your metricDir is in /tmp
	 */
	if _, err := os.Stat(conf.MetricDir); os.IsNotExist(err) {
		os.MkdirAll(conf.MetricDir, 0777|os.ModeSticky)
	} else {
		os.Chmod(conf.MetricDir, 0777|os.ModeSticky)
	}

	/* Buffers */
	var ch chan string = make(chan string, lc.mainBufferSize + monitorMetrics)
	var chM chan string = make(chan string, monitorMetrics)

	mon := NewMonitoring(conf, lg, chM)
	cli := NewClient(conf, lc, mon, lg, ch, chM)
	srv := NewServer(conf, lc, mon, lg, ch)

	go srv.runServer()
	go cli.runClient()

	mon.runMonitoring()
}