package client

import (
	"errors"
	"net"
	"bytes"
	"io"
	"fmt"
	"encoding/binary"
)

const (
	DmapPacketBodySize = 65520
	DmapPacketMagic = 0xCBEECBEE
	DmapKeySize = 16
	DmapValueSize = 4096
	DmapPacketSetKey = 4
	DmapPacketGetKey = 5
	DmapPacketDelKey = 6
	DmapPacketUpdKey = 7
	DmapPacketCmpxchgKey = 8
)

type Client struct {
	Host string
	Con net.Conn
}

type DmapPacketHeader struct {
	Magic uint32
	Type uint32
	Len uint32
	Result uint32
}

type DmapPacket struct {
	Header DmapPacketHeader
	Body []byte
}

type DmapToBytes interface {
	ToBytes() ([]byte, error)
}

type DmapParseBytes interface {
	ParseBytes(body []byte) error
}

type DmapReqSetKey struct {
	Key [DmapKeySize]byte
	Value [DmapValueSize]byte
}

type DmapRespSetKey struct {
	Padding uint64
}

type DmapReqGetKey struct {
	Key [DmapKeySize]byte
}

type DmapRespGetKey struct {
	Value [DmapValueSize]byte
}

type DmapReqDelKey struct {
	Key [DmapKeySize]byte
}

type DmapRespDelKey struct {
	Padding uint64
}

type DmapReqUpdKey struct {
	Key [DmapKeySize]byte
	Value [DmapValueSize]byte
}

type DmapRespUpdKey struct {
	Padding uint64
}

type DmapReqCmpxchgKey struct {
	Key [DmapKeySize]byte
	Exchange [DmapValueSize]byte
	Comparand [DmapValueSize]byte
}

type DmapRespCmpxchgKey struct {
	Value [DmapValueSize]byte
}

func (req *DmapReqSetKey) ToBytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, req)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (resp *DmapRespSetKey) ParseBytes(body []byte) error {
	err := binary.Read(bytes.NewReader(body), binary.LittleEndian, resp)
	if err != nil {
		return err
	}
	return nil
}

func (req *DmapReqGetKey) ToBytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, req)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (resp *DmapRespGetKey) ParseBytes(body []byte) error {
	err := binary.Read(bytes.NewReader(body), binary.LittleEndian, resp)
	if err != nil {
		return err
	}
	return nil
}

func (req *DmapReqDelKey) ToBytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, req)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (resp *DmapRespDelKey) ParseBytes(body []byte) error {
	err := binary.Read(bytes.NewReader(body), binary.LittleEndian, resp)
	if err != nil {
		return err
	}
	return nil
}

func (req *DmapReqUpdKey) ToBytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, req)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (resp *DmapRespUpdKey) ParseBytes(body []byte) error {
	err := binary.Read(bytes.NewReader(body), binary.LittleEndian, resp)
	if err != nil {
		return err
	}
	return nil
}

func (req *DmapReqCmpxchgKey) ToBytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, req)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (resp *DmapRespCmpxchgKey) ParseBytes(body []byte) error {
	err := binary.Read(bytes.NewReader(body), binary.LittleEndian, resp)
	if err != nil {
		return err
	}
	return nil
}

func (client *Client) Init(host string) {
	client.Host = host
}

func (client *Client) Dial() error {
	con, err := net.Dial("tcp", client.Host)
	if err != nil {
		return err
	}
	client.Con = con
	return nil
}

func (client *Client) CreatePacket(packetType uint32, body []byte) *DmapPacket {
	packet := new(DmapPacket)
	packet.Header.Magic = DmapPacketMagic
	packet.Header.Type = packetType
	packet.Header.Len = uint32(len(body))
	packet.Header.Result = 0
	packet.Body = body
	return packet
}

func (client *Client) SendPacket(packet *DmapPacket) error {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, &packet.Header)
	if err != nil {
		return err
	}

	err = binary.Write(buf, binary.LittleEndian, packet.Body)
	if err != nil {
		return err
	}

	n, err := client.Con.Write(buf.Bytes())
	if err != nil {
		return err
	}

	if n != buf.Len() {
		return errors.New("Incomplete I/O")
	}

	return nil
}

func (client *Client) RecvPacket() (*DmapPacket, error) {
	packet := new(DmapPacket)
	err := binary.Read(client.Con, binary.LittleEndian, &packet.Header)
	if err != nil {
		return nil, err
	}

	if packet.Header.Magic != DmapPacketMagic {
		return nil, errors.New("Invalid packet magic")
	}

	if packet.Header.Len > DmapPacketBodySize {
		return nil, errors.New("Packet body len too big")
	}

	body := make([]byte, packet.Header.Len)
	if packet.Header.Len != 0 {
		n, err := io.ReadFull(client.Con, body)
		if err != nil {
			return nil, err
		}

		if uint32(n) != packet.Header.Len {
			return nil, errors.New("Incomplete I/O")
		}
	}
	packet.Body = body

	return packet, nil
}

func (client *Client) MakePacket(reqType uint32, req DmapToBytes) (*DmapPacket, error) {
	body, err := req.ToBytes()
	if err != nil {
		return nil, err
	}
	return client.CreatePacket(reqType, body), nil
}

func (client *Client) SendRequest(reqType uint32, req DmapToBytes) error {
	packet, err := client.MakePacket(reqType, req)
	if err != nil {
		return err
	}

	return client.SendPacket(packet)
}

func (client *Client) RecvResponse(respType uint32, resp DmapParseBytes) error {
	packet, err := client.RecvPacket()
	if err != nil {
		return err
	}

	if packet.Header.Type != respType {
		return fmt.Errorf("Unexpected packet type %d, should be %d",
			packet.Header.Type, respType)
	}

	if packet.Header.Result != 0 {
		return fmt.Errorf("Packet error: %d", int32(packet.Header.Result))
	}

	return resp.ParseBytes(packet.Body)
}

func (client *Client) SendRecv(reqType uint32, req DmapToBytes, resp DmapParseBytes) error {
	err := client.SendRequest(reqType, req)
	if err != nil {
		return err
	}

	err = client.RecvResponse(reqType, resp)
	if err != nil {
		return err
	}

	return nil
}

func (client *Client) SetKey(key string, value string) error {
	req := new(DmapReqSetKey)
	resp := new(DmapRespSetKey)

	keyBytes := []byte(key)
	valueBytes := []byte(value)

	if len(keyBytes) > len(req.Key) {
		return errors.New("Key too big")
	}

	if len(valueBytes) > len(req.Value) {
		return errors.New("Value too big")
	}

	copy(req.Key[:len(req.Key)], keyBytes)
	copy(req.Value[:len(req.Value)], valueBytes)


	err := client.SendRecv(DmapPacketSetKey, req, resp)
	if err != nil {
		return err
	}

	return nil
}

func (client *Client) GetKey(key string) (string, error) {
	req := new(DmapReqGetKey)
	resp := new(DmapRespGetKey)

	keyBytes := []byte(key)
	if len(keyBytes) > len(req.Key) {
		return "", errors.New("Key too big")
	}

	copy(req.Key[:len(req.Key)], keyBytes)

	err := client.SendRecv(DmapPacketGetKey, req, resp)
	if err != nil {
		return "", err
	}

	return string(resp.Value[:len(resp.Value)]), nil
}

func (client *Client) DelKey(key string) error {
	req := new(DmapReqDelKey)
	resp := new(DmapRespDelKey)

	keyBytes := []byte(key)
	if len(keyBytes) > len(req.Key) {
		return errors.New("Key too big")
	}

	copy(req.Key[:len(req.Key)], keyBytes)

	err := client.SendRecv(DmapPacketDelKey, req, resp)
	if err != nil {
		return err
	}

	return nil
}

func (client *Client) UpdateKey(key string, value string) error {
	req := new(DmapReqUpdKey)
	resp := new(DmapRespUpdKey)

	keyBytes := []byte(key)
	valueBytes := []byte(value)

	if len(keyBytes) > len(req.Key) {
		return errors.New("Key too big")
	}

	if len(valueBytes) > len(req.Value) {
		return errors.New("Value too big")
	}

	copy(req.Key[:len(req.Key)], keyBytes)
	copy(req.Value[:len(req.Value)], valueBytes)


	err := client.SendRecv(DmapPacketUpdKey, req, resp)
	if err != nil {
		return err
	}

	return nil
}

func (client *Client) CmpxchgKey(key string, exchange string,
				comparand string) (string, error) {
	req := new(DmapReqCmpxchgKey)
	resp := new(DmapRespCmpxchgKey)

	keyBytes := []byte(key)
	exchangeBytes := []byte(exchange)
	comparandBytes := []byte(comparand)

	if len(keyBytes) > len(req.Key) {
		return "", errors.New("Key too big")
	}

	if len(exchangeBytes) > len(req.Exchange) {
		return "", errors.New("Exchage too big")
	}

	if len(comparandBytes) > len(req.Exchange) {
		return "", errors.New("Comparand too big")
	}

	copy(req.Key[:len(req.Key)], keyBytes)
	copy(req.Exchange[:len(req.Exchange)], exchangeBytes)
	copy(req.Comparand[:len(req.Comparand)], comparandBytes)

	err := client.SendRecv(DmapPacketCmpxchgKey, req, resp)
	if err != nil {
		return "", err
	}

	return string(resp.Value[:len(resp.Value)]), nil
}

func (client *Client) Close() {
	if client.Con != nil {
		client.Con.Close()
	}
}
