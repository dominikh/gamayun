package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

// TODO http://www.bittorrent.org/beps//bep_0006.html

type MessageType byte

const (
	MessageTypeChoke         MessageType = 0x00
	MessageTypeUnchoke       MessageType = 0x01
	MessageTypeInterested    MessageType = 0x02
	MessageTypeNotInterested MessageType = 0x03
	MessageTypeHave          MessageType = 0x04
	MessageTypeBitfield      MessageType = 0x05
	MessageTypeRequest       MessageType = 0x06
	MessageTypePiece         MessageType = 0x07
	MessageTypeCancel        MessageType = 0x08
	MessageTypeHaveAll       MessageType = 0x0e
	MessageTypeHaveNone      MessageType = 0x0f
	MessageTypeRejectRequest MessageType = 0x10
	MessageTypeAllowedFast   MessageType = 0x11

	MessageTypeKeepAlive MessageType = 0xFF // not used as a type in the protocol, zero-length messages are keep alives
)

const (
	ExtensionFast uint64 = 0x04
)

type Connection struct {
	Conn net.Conn

	readBuf  []byte
	writeBuf []byte

	// OPT(dh): use bitfield for extensions
	HasFastPeers bool
}

const protocol = "BitTorrent protocol"
const handshakeLength = 1 + len(protocol) + 8 + 20 + 20

var protocolBytes = []byte(protocol)

func ncopy(dst []byte, srcs ...[]byte) {
	off := 0
	for _, src := range srcs {
		copy(dst[off:], src)
		off += len(src)
		if off >= len(dst) {
			break
		}
	}
}

func NewConnection(conn net.Conn) *Connection {
	return &Connection{
		Conn:     conn,
		readBuf:  make([]byte, 32*1024+12),
		writeBuf: make([]byte, handshakeLength),
	}
}

func (conn *Connection) SendHandshake(infoHash, peerID [20]byte) error {
	b := conn.writeBuf[:handshakeLength]

	var reserved [8]byte
	binary.BigEndian.PutUint64(reserved[:], ExtensionFast)

	b[0] = byte(len(protocol))
	ncopy(b[1:], []byte(protocol), reserved[:], infoHash[:], peerID[:])
	_, err := conn.Conn.Write(b[:])
	return err
}

func (conn *Connection) ReadHandshake() ([20]byte, error) {
	b := conn.readBuf[:handshakeLength-20]

	_, err := io.ReadFull(conn.Conn, b[:])
	if err != nil {
		return [20]byte{}, err
	}
	if b[0] != byte(len(protocol)) || !bytes.Equal(b[1:], protocolBytes) {
		// Not a BitTorrent connection
		// XXX
	}

	if (b[8] & 0x04) != 0 {
		conn.HasFastPeers = true
	}

	var out [20]byte
	copy(out[:], b[1+len(protocol)+8:])
	return out, nil
}

func (conn *Connection) ReadPeerID() ([20]byte, error) {
	out := conn.readBuf[:20]
	_, err := io.ReadFull(conn.Conn, out[:])
	return *(*[20]byte)(out), err
}

type Message struct {
	Type MessageType

	Index  uint32 // Have, Request, Piece, Cancel
	Begin  uint32 // Request, Piece, Cancel
	Length uint32 // Request, Cancel
	Data   []byte // Bitfield, Piece
}

func (conn *Connection) ReadMessage() (Message, error) {
	// OPT(dh): use fewer read calls/use buffering

	lengthBuf := conn.readBuf[:4]
	if _, err := io.ReadFull(conn.Conn, lengthBuf[:]); err != nil {
		return Message{}, err
	}

	length := binary.BigEndian.Uint32(lengthBuf[:])
	if length == 0 {
		return Message{Type: MessageTypeKeepAlive}, nil
	}

	// TODO verify length. it doesn't have to be larger than 32 KiB + 12 bytes

	// XXX verify length isn't larger than readBuf
	b := conn.readBuf[:length]
	if _, err := io.ReadFull(conn.Conn, b); err != nil {
		return Message{}, err
	}

	msg := Message{Type: MessageType(b[0])}
	switch msg.Type {
	case MessageTypeChoke:
	case MessageTypeUnchoke:
	case MessageTypeInterested:
	case MessageTypeNotInterested:
	case MessageTypeHave:
		msg.Index = binary.BigEndian.Uint32(b[1:])
	case MessageTypeBitfield:
		msg.Data = b[1:]
	case MessageTypeRequest:
		msg.Index = binary.BigEndian.Uint32(b[1:])
		msg.Begin = binary.BigEndian.Uint32(b[5:])
		msg.Length = binary.BigEndian.Uint32(b[9:])
	case MessageTypePiece:
		msg.Index = binary.BigEndian.Uint32(b[1:])
		msg.Begin = binary.BigEndian.Uint32(b[5:])
		msg.Data = b[9:]
	case MessageTypeCancel:
		msg.Index = binary.BigEndian.Uint32(b[1:])
		msg.Begin = binary.BigEndian.Uint32(b[5:])
		msg.Length = binary.BigEndian.Uint32(b[9:])
	case MessageTypeHaveAll:
	case MessageTypeHaveNone:
	default:
		// XXX don't panic because of a protocol error
		panic(fmt.Sprintf("unhandled message %d", b[0]))
	}

	return msg, nil
}

func (conn *Connection) WriteMessage(msg Message) error {
	sb := conn.writeBuf
	*(*[4]byte)(sb) = [4]byte{0, 0, 0, 0}
	switch msg.Type {
	case MessageTypeKeepAlive:
		_, err := conn.Conn.Write(sb[:4])
		return err

	case MessageTypeChoke:
		sb[3] = 1
		sb[4] = byte(MessageTypeChoke)
		_, err := conn.Conn.Write(sb[:5])
		return err
	case MessageTypeUnchoke:
		sb[3] = 1
		sb[4] = byte(MessageTypeUnchoke)
		_, err := conn.Conn.Write(sb[:5])
		return err
	case MessageTypeInterested:
		sb[3] = 1
		sb[4] = byte(MessageTypeInterested)
		_, err := conn.Conn.Write(sb[:5])
		return err
	case MessageTypeNotInterested:
		sb[3] = 0x01
		sb[4] = byte(MessageTypeNotInterested)
		_, err := conn.Conn.Write(sb[:5])
		return err
	case MessageTypeHaveAll:
		sb[3] = 0x01
		sb[4] = byte(MessageTypeHaveAll)
		_, err := conn.Conn.Write(sb[:5])
		return err
	case MessageTypeHaveNone:
		sb[3] = 0x01
		sb[4] = byte(MessageTypeHaveNone)
		_, err := conn.Conn.Write(sb[:5])
		return err
	case MessageTypeHave:
		sb[3] = 5
		sb[4] = byte(MessageTypeHave)
		binary.BigEndian.PutUint32(sb[5:], msg.Index)
		_, err := conn.Conn.Write(sb[:9])
		return err
	case MessageTypeRequest:
		sb[3] = 13
		sb[4] = byte(MessageTypeRequest)
		binary.BigEndian.PutUint32(sb[5:], msg.Index)
		binary.BigEndian.PutUint32(sb[9:], msg.Begin)
		binary.BigEndian.PutUint32(sb[13:], msg.Length)
		_, err := conn.Conn.Write(sb[:17])
		return err
	case MessageTypeCancel:
		sb[3] = 13
		sb[4] = byte(MessageTypeCancel)
		binary.BigEndian.PutUint32(sb[5:], msg.Index)
		binary.BigEndian.PutUint32(sb[9:], msg.Begin)
		binary.BigEndian.PutUint32(sb[13:], msg.Length)
		_, err := conn.Conn.Write(sb[:17])
		return err
	case MessageTypeBitfield:
		binary.BigEndian.PutUint32(sb[:], uint32(len(msg.Data)+1))
		sb[4] = byte(MessageTypeBitfield)
		bufs := net.Buffers{sb[:5], msg.Data}
		_, err := bufs.WriteTo(conn.Conn)
		return err
	case MessageTypePiece:
		binary.BigEndian.PutUint32(sb[:], uint32(len(msg.Data)+1))
		sb[4] = byte(MessageTypePiece)
		binary.BigEndian.PutUint32(sb[5:], msg.Index)
		binary.BigEndian.PutUint32(sb[9:], msg.Begin)
		bufs := net.Buffers{sb[:13], msg.Data}
		_, err := bufs.WriteTo(conn.Conn)
		return err
	default:
		panic(fmt.Sprintf("unreachable: %s", msg.Type))
	}
}
