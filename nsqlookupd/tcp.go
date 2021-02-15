package main

import (
	"../nsq"
	"../util"
	"log"
	"net"
)

// 开始会发送一个 4 字节来表明协议版本号
// protocols 存储不同版本号对应的协议
type TcpProtocol struct {
	util.TcpHandler
	protocols map[int32]nsq.Protocol
}

func (p *TcpProtocol) Handle(clientConn net.Conn) {
	log.Printf("TCP: new client(%s)", clientConn.RemoteAddr())

	protocolMagic, err := nsq.ReadMagic(clientConn)
	if err != nil {
		log.Printf("ERROR: failed to read protocol version - %s", err.Error())
		return
	}

	log.Printf("CLIENT(%s): desired protocol %d", clientConn.RemoteAddr(), protocolMagic)

	prot, ok := p.protocols[protocolMagic]
	if !ok {
		nsq.SendResponse(clientConn, []byte("E_BAD_PROTOCOL"))
		log.Printf("ERROR: client(%s) bad protocol version %d", clientConn.RemoteAddr(), protocolMagic)
		return
	}

	err = prot.IOLoop(clientConn)
	if err != nil {
		log.Printf("ERROR: client(%s) - %s", clientConn.RemoteAddr(), err.Error())
		return
	}
}
