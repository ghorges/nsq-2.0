package main

import (
	"../nsq"
	"../util"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
)

var (
	showVersion = flag.Bool("version", false, "print version string")
	tcpAddress  = flag.String("tcp-address", "0.0.0.0:4160", "<addr>:<port> to listen on for TCP clients")
	httpAddress = flag.String("http-address", "0.0.0.0:4161", "<addr>:<port> to listen on for HTTP clients")
	debugMode   = flag.Bool("debug", false, "enable debug mode")
)

var protocols = map[int32]nsq.Protocol{}
var lookupd *NSQLookupd

func main() {
	flag.Parse()

	// 打印版本信息并返回
	if *showVersion {
		fmt.Printf("nsqlookupd v%s\n", util.BINARY_VERSION)
		return
	}

	// os.Signal 表示系统信号
	signalChan := make(chan os.Signal, 1)
	exitChan := make(chan int)

	// 接收到系统信号，关闭程序
	go func() {
		<-signalChan
		exitChan <- 1
	}()
	// 捕获 SIGINT 信号
	signal.Notify(signalChan, os.Interrupt)

	tcpAddr, err := net.ResolveTCPAddr("tcp", *tcpAddress)
	if err != nil {
		log.Fatal(err)
	}

	httpAddr, err := net.ResolveTCPAddr("tcp", *httpAddress)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("nsqlookupd v%s", util.BINARY_VERSION)

	lookupd = NewNSQLookupd()
	lookupd.tcpAddr = tcpAddr
	lookupd.httpAddr = httpAddr
	lookupd.Main()
	<-exitChan
	lookupd.Exit()
}
