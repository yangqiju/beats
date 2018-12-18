// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package dubbohession

import (
	//"bytes"
	"time"

	//"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/monitoring"

	//"github.com/elastic/beats/packetbeat/procs"
	"github.com/elastic/beats/packetbeat/protos"
	"github.com/elastic/beats/packetbeat/protos/tcp"
	"github.com/elastic/beats/libbeat/beat"
	"bytes"
	"bufio"
)

type stream struct {
	tcptuple *common.TCPTuple
	data     []byte
	message  *dubboHessionMessage
	isClient bool
}
type dubboHessionMessage struct {
	ts time.Time

	tcpTuple     common.TCPTuple
	cmdlineTuple *common.ProcessTuple
	direction    uint8

	isResponse      bool
	expectsResponse bool

	// Standard message header fields from mongodb wire protocol
	// see http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#standard-message-header
	messageLength int
	requestID     int
	responseTo    int
	//opCode        opCode

	// deduced from content. Either an operation from the original wire protocol or the name of a command (passed through a query)
	// List of commands: http://docs.mongodb.org/manual/reference/command/
	// List of original protocol operations: http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#request-opcodes
	method    string
	error     string
	resource  string
	documents []interface{}
	params    map[string]interface{}

	// Other fields vary very much depending on operation type
	// lets just put them in a map
	event common.MapStr
}

type dubbohessionConnectionData struct {
	streams [2]*stream
	//requests  messageList
	//responses messageList
}

// Redis protocol plugin
type dubboHessionPlugin struct {
	// config
	ports        []int
	sendRequest  bool
	sendResponse bool

	transactionTimeout time.Duration

	codec  hessianCodec

	results protos.Reporter
}

var (
	debugf  = logp.MakeDebug("dubbohession")
	isDebug = true
)

var (
	unmatchedResponses = monitoring.NewInt(nil, "dubbohession.unmatched_responses")
)

func init() {
	protos.Register("dubbohession", New)
}

func New(
	testMode bool,
	results protos.Reporter,
	cfg *common.Config,
) (protos.Plugin, error) {
	p := &dubboHessionPlugin{}
	config := defaultConfig
	if !testMode {
		if err := cfg.Unpack(&config); err != nil {
			return nil, err
		}
	}

	if err := p.init(results, &config); err != nil {
		return nil, err
	}
	return p, nil
}

type readWriteCloser struct {
	wbuf *bytes.Buffer
	rbuf *bytes.Buffer
}


func (rwc *readWriteCloser) Read(p []byte) (n int, err error) {
	return rwc.rbuf.Read(p)
}

func (rwc *readWriteCloser) Write(p []byte) (n int, err error) {
	return rwc.wbuf.Write(p)
}

func (rwc *readWriteCloser) Close() error {
	rwc.rbuf.Reset()
	rwc.wbuf.Reset()
	return nil
}

func (redis *dubboHessionPlugin) init(results protos.Reporter, config *dubbohessionConfig) error {
	redis.setFromConfig(config)
	rwc := &readWriteCloser{
		rbuf: bytes.NewBuffer(nil),
		wbuf: bytes.NewBuffer(nil),
	}

	hessianCodec := &hessianCodec{
		rwc:    rwc,
		reader: bufio.NewReader(rwc),
	}

	redis.codec = *hessianCodec

	redis.results = results
	isDebug = logp.IsDebug("dubbohession")

	return nil
}

func (redis *dubboHessionPlugin) setFromConfig(config *dubbohessionConfig) {
	redis.ports = config.Ports
	redis.sendRequest = config.SendRequest
	redis.sendResponse = config.SendResponse
	redis.transactionTimeout = config.TransactionTimeout
}

func (redis *dubboHessionPlugin) GetPorts() []int {
	return redis.ports
}

// Parser moves to next message in stream
func (st *stream) PrepareForNewMessage() {
	st.data = st.data[st.message.messageLength:]
	st.message = nil
}

func (redis *dubboHessionPlugin) ConnectionTimeout() time.Duration {
	return redis.transactionTimeout
}

func (redis *dubboHessionPlugin) Parse(
	pkt *protos.Packet,
	tcptuple *common.TCPTuple,
	dir uint8,
	private protos.ProtocolData,
) protos.ProtocolData {
	defer logp.Recover("Parse dubbohession exception")

	conn := ensureDubboHessionConnection(private)
	conn = redis.doParse(conn, pkt, tcptuple, dir)
	if conn == nil {
		return nil
	}
	return conn
}

func ensureDubboHessionConnection(private protos.ProtocolData) *dubbohessionConnectionData {
	if private == nil {
		return &dubbohessionConnectionData{}
	}

	priv, ok := private.(*dubbohessionConnectionData)
	if !ok {
		logp.Warn("redis connection data type error, create new one")
		return &dubbohessionConnectionData{}
	}
	if priv == nil {
		logp.Warn("Unexpected: redis connection data not set, create new one")
		return &dubbohessionConnectionData{}
	}

	return priv
}

func (redis *dubboHessionPlugin) doParse(
	conn *dubbohessionConnectionData,
	pkt *protos.Packet,
	tcptuple *common.TCPTuple,
	dir uint8,
) *dubbohessionConnectionData {

	st := conn.streams[dir]
	if st == nil {

		dstPort := tcptuple.DstPort
		if dir == tcp.TCPDirectionReverse {
			dstPort = tcptuple.SrcPort
		}

		st = &stream{
			tcptuple: tcptuple,
			data:     pkt.Payload,
			message:  &dubboHessionMessage{ts: pkt.Ts},
			isClient: redis.isServerPort(dstPort),
		}
		conn.streams[dir] = st
		if isDebug {
			debugf("new stream: %p (dir=%v, len=%v)", st, dir, len(pkt.Payload))
		}
	} else {
		// concatenate bytes
		st.data = append(st.data, pkt.Payload...)
		if len(st.data) > tcp.TCPMaxDataInStream {
			debugf("Stream data too large, dropping TCP stream")
			conn.streams[dir] = nil
			return conn
		}
	}

	if isDebug {
		debugf("stream add data: %p (dir=%v, len=%v)", st, dir, len(pkt.Payload))
	}

	for len(st.data) > 0 {
		if st.message == nil {
			st.message = &dubboHessionMessage{ts: pkt.Ts}
		}

		ok, complete := redis.dubbohessionMessageParser(st)
		if !ok {
			// drop this tcp stream. Will retry parsing with the next
			// segment in it
			conn.streams[dir] = nil
			debugf("Ignore  message. Drop tcp stream. Try parsing with the next segment")
			return conn
		}

		if !complete {
			// wait for more data
			debugf(" wait for more data before parsing message")
			break
		}
		debugf("message complete")
		//mongodb.handleMongodb(conn, st.message, tcptuple, dir)
		//TODO
		fields := common.MapStr{}
		fields["type"] = "dubbohession"
		fields["method"] = st.message.method
		redis.results(beat.Event{
			Timestamp: pkt.Ts,
			Fields:    fields,
		})
		st.PrepareForNewMessage()
	}
	return conn
}

func (mysql *dubboHessionPlugin) isServerPort(port uint16) bool {
	for _, sPort := range mysql.ports {
		if uint16(sPort) == port {
			return true
		}
	}
	return false
}
func (redis *dubboHessionPlugin) GapInStream(tcptuple *common.TCPTuple, dir uint8,
	nbytes int, private protos.ProtocolData) (priv protos.ProtocolData, drop bool) {

	// tsg: being packet loss tolerant is probably not very useful for Redis,
	// because most requests/response tend to fit in a single packet.

	return private, true
}

func (redis *dubboHessionPlugin) ReceivedFin(tcptuple *common.TCPTuple, dir uint8,
	private protos.ProtocolData) protos.ProtocolData {

	// TODO: check if we have pending data that we can send up the stack

	return private
}
