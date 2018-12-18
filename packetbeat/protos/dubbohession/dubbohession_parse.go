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
	"encoding/json"
	"errors"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	log "github.com/AlexStocks/log4go"
	"gopkg.in/mgo.v2/bson"
	"strconv"
	"encoding/hex"
	"encoding/binary"
)

func (redis *dubboHessionPlugin) dubbohessionMessageParser(s *stream) (bool, bool) {
	//2byte magic:类似java字节码文件里的魔数，用来判断是不是dubbo协议的数据包。魔数是常量0xdabb
	//1byte 的消息标志位:16-20序列id,21 event,22 two way,23请求或响应标识
	//1byte 状态，当消息类型为响应时，设置响应状态。24-31位。状态位, 设置请求响应状态，dubbo定义了一些响应的类型。具体类型见com.alibaba.dubbo.remoting.exchange.Response
	//8byte 消息ID,long类型，32-95位。每一个请求的唯一识别id（由于采用异步通讯的方式，用来把请求request和返回的response对应上）
	//4byte 消息长度，96-127位。消息体 body 长度, int 类型，即记录Body Content有多少个字节

	if 16 > len(s.data) {
		// Not yet reached the end of message
		log.Warn("长度不够:")
		return true, false
	}

	d := newDecoder(s.data)

	magic, err := d.readBytes(2);
	log.Warn("magic:" + string(magic[:]))
	if magic[0] != byte(MAGIC_HIGH) && magic[1] != byte(MAGIC_LOW) {
		log.Warn("magic 不匹配")
	} else {
		hex.EncodeToString(magic)
		log.Warn("magic 匹配:"+hex.EncodeToString(magic))
	}
	_, err = d.readBytes(2);
	requestIdBytes, err := d.readBytes(8);
	requestId := int64(binary.BigEndian.Uint64(requestIdBytes))
	//requestId, err := d.readInt64();

	log.Warn("requestId:" + strconv.FormatInt(requestId,10))
	if err != nil {
		return true, false
	}
	dataLengthBytes, err := d.readBytes(4);
	bodyLength := int(binary.BigEndian.Uint32(dataLengthBytes))
	if err != nil {
		return true, false
	}
	log.Warn("读取body长度:" + strconv.Itoa(bodyLength))
	//body
	_, err = d.readBytes(int32(bodyLength));
	//responseBody := unpackResponseBody(body, &rsp)

	s.message.messageLength = 16 + bodyLength
	//s.message.requestID = requestId

	//	cm Message
	//	rsp   string
	//)
	//redis.codec.rwc.Write(s.data);
	//err := redis.codec.ReadHeader(&cm, Response)
	//if err != nil {
	//	return false, false
	//}
	//
	//if cm.BodyLen > len(s.data) {
	//	// Not yet reached the end of message
	//	return true, false
	//}
	//
	//redis.codec.ReadBody(&rsp);

	//s.message.error = cm.Error
	//s.message.method = cm.Method
	//s.message.messageLength = cm.BodyLen
	//
	//log.Warn("method::" + s.message.method)

	//TODO
	//opCode := opCode(code)
	//
	//if !validOpcode(opCode) {
	//	logp.Err("Unknown operation code: %v", opCode)
	//	return false, false
	//}
	//
	//s.message.opCode = opCode
	s.message.isResponse = false // default is that the message is a request. If not opReplyParse will set this to false
	s.message.expectsResponse = false
	//debugf("opCode = %v", s.message.opCode)

	// then split depending on operation type
	s.message.event = common.MapStr{}

	//switch s.message.opCode {
	//case opReply:
	//	s.message.isResponse = true
	//	return opReplyParse(d, s.message)
	//case opMsg:
	//	s.message.method = "msg"
	//	return opMsgParse(d, s.message)
	//case opUpdate:
	//	s.message.method = "update"
	//	return opUpdateParse(d, s.message)
	//case opInsert:
	//	s.message.method = "insert"
	//	return opInsertParse(d, s.message)
	//case opQuery:
	//	s.message.expectsResponse = true
	//	return opQueryParse(d, s.message)
	//case opGetMore:
	//	s.message.method = "getMore"
	//	s.message.expectsResponse = true
	//	return opGetMoreParse(d, s.message)
	//case opDelete:
	//	s.message.method = "delete"
	//	return opDeleteParse(d, s.message)
	//case opKillCursor:
	//	s.message.method = "killCursors"
	//	return opKillCursorsParse(d, s.message)
	//}

	//return false, false

	return true, true;
}

// see http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#op-reply
func opReplyParse(d *decoder, m *dubboHessionMessage) (bool, bool) {
	_, err := d.readInt32() // ignore flags for now
	m.event["cursorId"], err = d.readInt64()
	m.event["startingFrom"], err = d.readInt32()

	numberReturned, err := d.readInt32()
	m.event["numberReturned"] = numberReturned

	debugf("Prepare to read %d document from reply", m.event["numberReturned"])

	documents := make([]interface{}, numberReturned)
	for i := 0; i < numberReturned; i++ {
		var document bson.M
		document, err = d.readDocument()

		// Check if the result is actually an error
		if i == 0 {
			if mongoError, present := document["$err"]; present {
				m.error, err = doc2str(mongoError)
			}

			if writeErrors, present := document["writeErrors"]; present {
				m.error, err = doc2str(writeErrors)
			}
		}

		documents[i] = document
	}
	m.documents = documents

	if err != nil {
		logp.Err("An error occurred while parsing OP_REPLY message: %s", err)
		return false, false
	}
	return true, true
}

func opMsgParse(d *decoder, m *dubboHessionMessage) (bool, bool) {
	var err error
	m.event["message"], err = d.readCStr()
	if err != nil {
		logp.Err("An error occurred while parsing OP_MSG message: %s", err)
		return false, false
	}
	return true, true
}

func opUpdateParse(d *decoder, m *dubboHessionMessage) (bool, bool) {
	_, err := d.readInt32() // always ZERO, a slot reserved in the protocol for future use
	m.event["fullCollectionName"], err = d.readCStr()
	_, err = d.readInt32() // ignore flags for now

	m.event["selector"], err = d.readDocumentStr()
	m.event["update"], err = d.readDocumentStr()

	if err != nil {
		logp.Err("An error occurred while parsing OP_UPDATE message: %s", err)
		return false, false
	}

	return true, true
}

func opInsertParse(d *decoder, m *dubboHessionMessage) (bool, bool) {
	_, err := d.readInt32() // ignore flags for now
	m.event["fullCollectionName"], err = d.readCStr()

	// TODO parse bson documents
	// Not too bad if it is not done, as all recent mongodb clients send insert as a command over a query instead of this
	// Find an old client to generate a pcap with legacy protocol ?

	if err != nil {
		logp.Err("An error occurred while parsing OP_INSERT message: %s", err)
		return false, false
	}

	return true, true
}

func extractDocuments(query map[string]interface{}) []interface{} {
	docsVi, present := query["documents"]
	if !present {
		return []interface{}{}
	}

	docs, ok := docsVi.([]interface{})
	if !ok {
		return []interface{}{}
	}
	return docs
}

func opGetMoreParse(d *decoder, m *dubboHessionMessage) (bool, bool) {
	_, err := d.readInt32() // always ZERO, a slot reserved in the protocol for future use
	m.event["fullCollectionName"], err = d.readCStr()
	m.event["numberToReturn"], err = d.readInt32()
	m.event["cursorId"], err = d.readInt64()

	if err != nil {
		logp.Err("An error occurred while parsing OP_GET_MORE message: %s", err)
		return false, false
	}
	return true, true
}

func opDeleteParse(d *decoder, m *dubboHessionMessage) (bool, bool) {
	_, err := d.readInt32() // always ZERO, a slot reserved in the protocol for future use
	m.event["fullCollectionName"], err = d.readCStr()
	_, err = d.readInt32() // ignore flags for now

	m.event["selector"], err = d.readDocumentStr()

	if err != nil {
		logp.Err("An error occurred while parsing OP_DELETE message: %s", err)
		return false, false
	}

	return true, true
}

func opKillCursorsParse(d *decoder, m *dubboHessionMessage) (bool, bool) {
	// TODO ? Or not, content is not very interesting.
	return true, true
}

// NOTE: The following functions are inspired by the source of the go-mgo/mgo project
// https://github.com/go-mgo/mgo/blob/v2/bson/decode.go

type decoder struct {
	in []byte
	i  int
}

func newDecoder(in []byte) *decoder {
	return &decoder{in, 0}
}

func (d *decoder) truncate(length int) {
	d.in = d.in[:length]
}

func (d *decoder) readCStr() (string, error) {
	start := d.i
	end := start
	l := len(d.in)
	for ; end != l; end++ {
		if d.in[end] == '\x00' {
			break
		}
	}
	d.i = end + 1
	if d.i > l {
		return "", errors.New("cstring not finished")
	}
	return string(d.in[start:end]), nil
}

func (d *decoder) readInt32() (int, error) {
	b, err := d.readBytes(4)

	if err != nil {
		return 0, err
	}

	return int((uint32(b[0]) << 0) |
		(uint32(b[1]) << 8) |
		(uint32(b[2]) << 16) |
		(uint32(b[3]) << 24)), nil
}

func (d *decoder) readInt64() (int, error) {
	b, err := d.readBytes(8)

	if err != nil {
		return 0, err
	}

	return int((uint64(b[0]) << 0) |
		(uint64(b[1]) << 8) |
		(uint64(b[2]) << 16) |
		(uint64(b[3]) << 24) |
		(uint64(b[4]) << 32) |
		(uint64(b[5]) << 40) |
		(uint64(b[6]) << 48) |
		(uint64(b[7]) << 56)), nil
}

func (d *decoder) readDocument() (bson.M, error) {
	start := d.i
	documentLength, err := d.readInt32()
	d.i = start + documentLength
	if len(d.in) < d.i {
		return nil, errors.New("document out of bounds")
	}

	documentMap := bson.M{}

	debugf("Parse %d bytes document from remaining %d bytes", documentLength, len(d.in)-start)
	err = bson.Unmarshal(d.in[start:d.i], documentMap)

	if err != nil {
		debugf("Unmarshall error %v", err)
		return nil, err
	}

	return documentMap, err
}

func doc2str(documentMap interface{}) (string, error) {
	document, err := json.Marshal(documentMap)
	return string(document), err
}

func (d *decoder) readDocumentStr() (string, error) {
	documentMap, err := d.readDocument()
	document, err := doc2str(documentMap)
	return document, err
}

func (d *decoder) readBytes(length int32) ([]byte, error) {
	start := d.i
	d.i += int(length)
	if d.i > len(d.in) {
		return *new([]byte), errors.New("No byte to read")
	}
	return d.in[start: start+int(length)], nil
}
