package main

import (
	"../nsq"
	"../util"
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type LookupProtocolV1 struct {
	nsq.Protocol
}

// v1 版本
func init() {
	// BigEndian client byte sequence "  V1"
	var magicInt int32
	buf := bytes.NewBuffer([]byte(nsq.MagicV1))
	binary.Read(buf, binary.BigEndian, &magicInt)
	// protocols 赋值
	protocols[magicInt] = &LookupProtocolV1{}
}

// 各自client是不同的 IOLoop，负责自己 tcp 循环的函数
func (p *LookupProtocolV1) IOLoop(conn net.Conn) error {
	var err error
	var line string

	client := NewClientV1(conn)
	client.State = nsq.StateInit
	err = nil
	reader := bufio.NewReader(client)
	for {
		// 以 \n 为特殊字符分割
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)
		params := strings.Split(line, " ")

		response, err := p.Exec(client, reader, params)
		if err != nil {
			log.Printf("ERROR: CLIENT(%s) - %s", client, err.(*nsq.ClientErr).Description())
			_, err = nsq.SendResponse(client, []byte(err.Error()))
			if err != nil {
				break
			}
			continue
		}

		if response != nil {
			_, err = nsq.SendResponse(client, response)
			if err != nil {
				break
			}
		}
	}

	log.Printf("CLIENT(%s): closing", client)
	if client.Producer != nil {
		lookupd.DB.Remove(Registration{"client", "", ""}, client.Producer)
		registrations := lookupd.DB.LookupRegistrations(client.Producer)
		for _, r := range registrations {
			lookupd.DB.Remove(*r, client.Producer)
		}
	}
	return err
}

func (p *LookupProtocolV1) Exec(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	switch params[0] {
	case "PING":
		return p.PING(client, params)
	case "IDENTIFY":
		return p.IDENTIFY(client, reader, params[1:])
	case "REGISTER":
		return p.REGISTER(client, reader, params[1:])
	case "UNREGISTER":
		return p.UNREGISTER(client, reader, params[1:])
	case "ANNOUNCE":
		return p.ANNOUNCE_OLD(client, reader, params[1:])
	}
	return nil, nsq.NewClientErr("E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

// 解析 topic 和 channel
func getTopicChan(params []string) (string, string, error) {
	if len(params) == 0 {
		return "", "", nsq.NewClientErr("E_MISSING_PARAMS", "insufficient number of params")
	}

	topicName := params[0]
	var channelName string
	if len(params) >= 2 {
		channelName = params[1]
	}

	if !nsq.IsValidTopicName(topicName) {
		return "", "", nsq.NewClientErr("E_BAD_TOPIC", fmt.Sprintf("topic name '%s' is not valid", topicName))
	}

	if channelName != "" && !nsq.IsValidChannelName(channelName) {
		return "", "", nsq.NewClientErr("E_BAD_CHANNEL", fmt.Sprintf("channel name '%s' is not valid", channelName))
	}

	return topicName, channelName, nil
}

func (p *LookupProtocolV1) REGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.Producer == nil {
		return nil, nsq.NewClientErr("E_INVALID", "client must IDENTIFY")
	}

	topic, channel, err := getTopicChan(params)
	if err != nil {
		return nil, err
	}

	// channel 不为空的时候，看起来注册了两次？
	if channel != "" {
		log.Printf("DB: client(%s) added registration for channel:%s in topic:%s", client, channel, topic)
		key := Registration{"channel", topic, channel}
		lookupd.DB.Add(key, client.Producer)
	}
	log.Printf("DB: client(%s) added registration for topic:%s", client, topic)
	key := Registration{"topic", topic, ""}
	lookupd.DB.Add(key, client.Producer)

	return []byte("OK"), nil
}

// 取消注册
func (p *LookupProtocolV1) UNREGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.Producer == nil {
		return nil, nsq.NewClientErr("E_INVALID", "client must IDENTIFY")
	}

	topic, channel, err := getTopicChan(params)
	if err != nil {
		return nil, err
	}

	if channel != "" {
		log.Printf("DB: client(%s) removed registration for channel:%s in topic:%s", client, channel, topic)
		key := Registration{"channel", topic, channel}
		producers := lookupd.DB.Remove(key, client.Producer)
		// for ephemeral channels, remove the channel as well if it has no producers
		if producers == 0 && strings.HasSuffix(channel, "#ephemeral") {
			lookupd.DB.RemoveRegistration(key)
		}
	}

	return []byte("OK"), nil
}

func (p *LookupProtocolV1) ANNOUNCE_OLD(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if len(params) < 3 {
		return nil, nsq.NewClientErr("E_MISSING_PARAMS", "insufficient number of params")
	}
	if len(params) >= 2 && params[1] == "." {
		params[1] = ""
	}
	topic, channel, err := getTopicChan(params)
	if err != nil {
		return nil, err
	}

	var bodyLen int32
	err = binary.Read(reader, binary.BigEndian, &bodyLen)
	if err != nil {
		return nil, nsq.NewClientErr("E_BAD_BODY", err.Error())
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return nil, nsq.NewClientErr("E_BAD_BODY", err.Error())
	}

	if client.Producer == nil {
		tcpPort, err := strconv.Atoi(params[2])
		if err != nil {
			return nil, nsq.NewClientErr("E_INVALID", fmt.Sprintf("could not parse TCP port %s", params[2]))
		}
		httpPort := tcpPort + 1
		if len(params) > 3 {
			httpPort, err = strconv.Atoi(params[4])
			if err != nil {
				return nil, nsq.NewClientErr("E_INVALID", fmt.Sprintf("could not parse HTTP port %s", params[4]))
			}
		}

		var ipAddrs []string
		// client sends multiple source IP address as the message body
		for _, ip := range bytes.Split(body, []byte("\n")) {
			ipAddrs = append(ipAddrs, string(ip))
		}

		client.Producer = &Producer{
			producerId: client.RemoteAddr().String(),
			TcpPort:    tcpPort,
			HttpPort:   httpPort,
			Address:    ipAddrs[len(ipAddrs)-1],
			LastUpdate: time.Now(),
		}
		log.Printf("CLIENT(%s): registered TCP:%d HTTP:%d address:%s",
			client, tcpPort, httpPort, client.Producer.Address)
		lookupd.DB.Add(Registration{"client", "", ""}, client.Producer)
	}

	var key Registration

	if channel != "" {
		log.Printf("DB: client(%s) added registration for channel:%s in topic:%s", client, channel, topic)
		key = Registration{"channel", topic, channel}
		lookupd.DB.Add(key, client.Producer)
	}

	log.Printf("DB: client(%s) added registration for topic:%s", client, topic)
	key = Registration{"topic", topic, ""}
	lookupd.DB.Add(key, client.Producer)

	return []byte("OK"), nil
}

// 生产者发送自己的 Producer，lookup 返回本机的 tcp_port、http_port、version、address
func (p *LookupProtocolV1) IDENTIFY(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	var err error

	var bodyLen int32
	err = binary.Read(reader, binary.BigEndian, &bodyLen)
	if err != nil {
		return nil, nsq.NewClientErr("E_BAD_BODY", err.Error())
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return nil, nsq.NewClientErr("E_BAD_BODY", err.Error())
	}

	// body is a json structure with producer information
	producer := Producer{producerId: client.RemoteAddr().String()}
	err = json.Unmarshal(body, &producer)
	if err != nil {
		return nil, nsq.NewClientErr("E_BAD_BODY", err.Error())
	}

	// require all fields
	if producer.Address == "" || producer.TcpPort == 0 || producer.HttpPort == 0 || producer.Version == "" {
		return nil, nsq.NewClientErr("E_BAD_BODY", "missing fields in IDENTIFY")
	}
	producer.LastUpdate = time.Now()

	client.Producer = &producer
	lookupd.DB.Add(Registration{"client", "", ""}, client.Producer)
	log.Printf("CLIENT(%s) registered TCP:%d HTTP:%d address:%s",
		client.RemoteAddr(),
		producer.TcpPort,
		producer.HttpPort,
		producer.Address)

	// build a response
	data := make(map[string]interface{})
	data["tcp_port"] = lookupd.tcpAddr.Port
	data["http_port"] = lookupd.httpAddr.Port
	data["version"] = util.BINARY_VERSION
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %s", err.Error())
	}
	data["address"] = hostname
	response, err := json.Marshal(data)
	if err != nil {
		log.Printf("ERROR: marshaling %v", data)
		return []byte("OK"), nil
	}
	return response, nil
}

// 处理 PING，并更新时间
func (p *LookupProtocolV1) PING(client *ClientV1, params []string) ([]byte, error) {
	if client.Producer != nil {
		// we could get a PING before an ANNOUNCE on the same client connection
		now := time.Now()
		log.Printf("CLIENT(%s): pinged (last ping %s)", client.Producer.producerId, now.Sub(client.Producer.LastUpdate))
		client.Producer.LastUpdate = now
	}
	return []byte("OK"), nil
}
