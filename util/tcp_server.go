package util

import (
	"log"
	"net"
	"runtime"
	"strings"
)

type TcpHandler interface {
	Handle(net.Conn)
}

// 监听并连接不同的 handle
func TcpServer(listener net.Listener, handler TcpHandler) {
	log.Printf("TCP: listening on %s", listener.Addr().String())

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				log.Printf("NOTICE: temporary Accept() failure - %s", err.Error())
				// 让出当前时间片？为什么要让？
				runtime.Gosched()
				continue
			}
			// theres no direct way to detect this error because it is not exposed
			if !strings.Contains(err.Error(), "use of closed network connection") {
				log.Printf("ERROR: listener.Accept() - %s", err.Error())
			}
			break
		}
		go handler.Handle(clientConn)
	}

	log.Printf("TCP: closing %s", listener.Addr().String())
}
