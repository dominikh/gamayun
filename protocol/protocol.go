package protocol

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

type InfoHash [20]byte

func (hash InfoHash) String() string { return fmt.Sprintf("%02x", [20]byte(hash)) }

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
	MessageTypeRejectRequest MessageType = 0x10
	MessageTypeAllowedFast   MessageType = 0x11
	MessageTypeSuggestPiece  MessageType = 0x0D
	MessageTypeHaveAll       MessageType = 0x0e
	MessageTypeHaveNone      MessageType = 0x0f

	MessageTypeKeepAlive MessageType = 0xFF // not used as a type in the protocol, zero-length messages are keep alives
)

const (
	ExtensionFast uint64 = 0x04
)

type Connection struct {
	Conn net.Conn

	readBuf  []byte
	writeBuf [][handshakeLength]byte

	// OPT(dh): use bitfield for extensions
	HasFastPeers bool
}

func (conn *Connection) Close() error { return conn.Conn.Close() }

func (conn *Connection) String() string {
	return fmt.Sprintf("%s -> %s", conn.Conn.RemoteAddr().String(), conn.Conn.LocalAddr().String())
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
		Conn:    conn,
		readBuf: make([]byte, 32*1024+12),
		// XXX "128" is shared between WriteMessages and runPeerUpload. make this more dynamic
		writeBuf: make([][handshakeLength]byte, 128),
	}
}

func (conn *Connection) SendHandshake(infoHash, peerID [20]byte) error {
	b := conn.writeBuf[0]

	var reserved [8]byte
	binary.BigEndian.PutUint64(reserved[:], ExtensionFast)

	b[0] = byte(len(protocol))
	ncopy(b[1:], []byte(protocol), reserved[:], infoHash[:], peerID[:])
	_, err := conn.Conn.Write(b[:])
	return err
}

const handshakeReadTimeout = 10 * time.Second

// ReadHandshake reads the handshake and returns the info hash.
func (conn *Connection) ReadHandshake() (InfoHash, error) {
	b := conn.readBuf[:handshakeLength-20]

	conn.Conn.SetReadDeadline(time.Now().Add(handshakeReadTimeout))
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

func (msg Message) String() string {
	switch msg.Type {
	case MessageTypeChoke:
		return "choke"
	case MessageTypeUnchoke:
		return "unchoke"
	case MessageTypeInterested:
		return "interested"
	case MessageTypeNotInterested:
		return "not-interested"
	case MessageTypeHave:
		return fmt.Sprintf("have(index=%d)", msg.Index)
	case MessageTypeBitfield:
		return fmt.Sprintf("bitfield(data=%d bytes)", len(msg.Data))
	case MessageTypeRequest:
		return fmt.Sprintf("request(index=%d, begin=%d, length=%d)", msg.Index, msg.Begin, msg.Length)
	case MessageTypePiece:
		return fmt.Sprintf("piece(index=%d, begin=%d, data=%d bytes)", msg.Index, msg.Begin, len(msg.Data))
	case MessageTypeCancel:
		return fmt.Sprintf("cancel(index=%d, begin=%d, length=%d)", msg.Index, msg.Begin, msg.Length)
	case MessageTypeRejectRequest:
		return fmt.Sprintf("reject-request((index=%d, begin=%d, length=%d)", msg.Index, msg.Begin, msg.Length)
	case MessageTypeAllowedFast:
		return fmt.Sprintf("allowed-fast(index=%d)", msg.Index)
	case MessageTypeSuggestPiece:
		return fmt.Sprintf("suggest-piece(index=%d)", msg.Index)
	case MessageTypeHaveAll:
		return "have-all"
	case MessageTypeHaveNone:
		return "have-none"
	case MessageTypeKeepAlive:
		return "keep-alive"
	default:
		return fmt.Sprintf("unknown-message(%d)", msg.Type)
	}
}

func (conn *Connection) ReadMessage(ctx context.Context) (Message, error) {
	// XXX sensible timeout
	conn.Conn.SetReadDeadline(time.Time{})
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

	// XXX verify that length matches message type
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
	case MessageTypeRejectRequest:
		msg.Index = binary.BigEndian.Uint32(b[1:])
		msg.Begin = binary.BigEndian.Uint32(b[5:])
		msg.Length = binary.BigEndian.Uint32(b[9:])
	case MessageTypeAllowedFast:
		msg.Index = binary.BigEndian.Uint32(b[1:])
	case MessageTypeSuggestPiece:
		msg.Index = binary.BigEndian.Uint32(b[1:])
	case MessageTypeHaveAll:
	case MessageTypeHaveNone:
	default:
		// XXX don't panic because of a protocol error
		panic(fmt.Sprintf("unhandled message %d", b[0]))
	}

	return msg, nil
}

func (msg Message) Size() int {
	sizes := [...]int{
		MessageTypeKeepAlive:     0,
		MessageTypeChoke:         1,
		MessageTypeUnchoke:       1,
		MessageTypeInterested:    1,
		MessageTypeNotInterested: 1,
		MessageTypeHaveAll:       1,
		MessageTypeHaveNone:      1,
		MessageTypeHave:          5,
		MessageTypeRequest:       13,
		MessageTypeRejectRequest: 13,
		MessageTypeCancel:        13,
	}

	switch msg.Type {
	case MessageTypeBitfield:
		return 4 + len(msg.Data) + 1
	case MessageTypePiece:
		return 4 + len(msg.Data) + 1 + 8
	default:
		return 4 + sizes[msg.Type]
	}
}

func (conn *Connection) WriteMessages(msgs []Message) error {
	bufs := make(net.Buffers, 0, len(msgs))

	sbs := conn.writeBuf
	for i, msg := range msgs {
		// sb := conn.writeBuf

		sb := sbs[i]
		switch msg.Type {
		case MessageTypeKeepAlive:
			bufs = append(bufs, sb[:4])
		case MessageTypeChoke:
			sb[3] = 1
			sb[4] = byte(MessageTypeChoke)
			bufs = append(bufs, sb[:5])
		case MessageTypeUnchoke:
			sb[3] = 1
			sb[4] = byte(MessageTypeUnchoke)
			bufs = append(bufs, sb[:5])
		case MessageTypeInterested:
			sb[3] = 1
			sb[4] = byte(MessageTypeInterested)
			bufs = append(bufs, sb[:5])
		case MessageTypeNotInterested:
			sb[3] = 0x01
			sb[4] = byte(MessageTypeNotInterested)
			bufs = append(bufs, sb[:5])
		case MessageTypeHaveAll:
			sb[3] = 0x01
			sb[4] = byte(MessageTypeHaveAll)
			bufs = append(bufs, sb[:5])
		case MessageTypeHaveNone:
			sb[3] = 0x01
			sb[4] = byte(MessageTypeHaveNone)
			bufs = append(bufs, sb[:5])
		case MessageTypeHave:
			sb[3] = 5
			sb[4] = byte(MessageTypeHave)
			binary.BigEndian.PutUint32(sb[5:], msg.Index)
			bufs = append(bufs, sb[:9])
		case MessageTypeRequest:
			sb[3] = 13
			sb[4] = byte(MessageTypeRequest)
			binary.BigEndian.PutUint32(sb[5:], msg.Index)
			binary.BigEndian.PutUint32(sb[9:], msg.Begin)
			binary.BigEndian.PutUint32(sb[13:], msg.Length)
			bufs = append(bufs, sb[:17])
		case MessageTypeRejectRequest:
			sb[3] = 13
			sb[4] = byte(MessageTypeRejectRequest)
			binary.BigEndian.PutUint32(sb[5:], msg.Index)
			binary.BigEndian.PutUint32(sb[9:], msg.Begin)
			binary.BigEndian.PutUint32(sb[13:], msg.Length)
			bufs = append(bufs, sb[:17])
		case MessageTypeCancel:
			sb[3] = 13
			sb[4] = byte(MessageTypeCancel)
			binary.BigEndian.PutUint32(sb[5:], msg.Index)
			binary.BigEndian.PutUint32(sb[9:], msg.Begin)
			binary.BigEndian.PutUint32(sb[13:], msg.Length)
			bufs = append(bufs, sb[:17])
		case MessageTypeBitfield:
			binary.BigEndian.PutUint32(sb[:], uint32(len(msg.Data)+1))
			sb[4] = byte(MessageTypeBitfield)
			bufs = append(bufs, sb[:5], msg.Data)
		case MessageTypePiece:
			binary.BigEndian.PutUint32(sb[:], uint32(len(msg.Data)+1+8))
			sb[4] = byte(MessageTypePiece)
			binary.BigEndian.PutUint32(sb[5:], msg.Index)
			binary.BigEndian.PutUint32(sb[9:], msg.Begin)
			bufs = append(bufs, sb[:13], msg.Data)
		default:
			panic(fmt.Sprintf("unreachable: %s", msg.Type))
		}
	}

	_, err := bufs.WriteTo(conn.Conn)
	return err
}

func (conn *Connection) WriteMessage(msg Message) error {
	// XXX add locking

	return conn.WriteMessages([]Message{msg})
}
