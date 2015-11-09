package mysql

import (
	"bufio"
	"fmt"
	"io"
	"net"
)

type PacketIO struct {
	rb *bufio.Reader
	wb io.Writer

	Sequence uint8
}

type PacketSection struct {
	Buffer       []byte
	NextPkgLen   uint32
	NextPkgState uint8
}

func NewPacketIO(conn net.Conn) *PacketIO {
	p := new(PacketIO)

	p.rb = bufio.NewReaderSize(conn, 1024)
	p.wb = conn

	p.Sequence = 0

	return p
}

func (p *PacketIO) WritePacketDirect(data []byte) error {
	count := len(data)
	if count == 0 {
		return fmt.Errorf("packet is nill")
	}
	if n, err := p.wb.Write(data); err != nil {
		return ErrBadConn
	} else if n != count {
		return ErrBadConn
	}
	return nil
}

//
func (p *PacketIO) ReadPacketBySection(length uint32, withHeader bool) (*PacketSection, error) {
	if length <= 0 {
		return nil, nil
	}
	ps := new(PacketSection)
	if withHeader {
		ps.Buffer = make([]byte, uint32(length+4))
	} else {
		ps.Buffer = make([]byte, uint32(length))
	}

	if _, err := io.ReadFull(p.rb, ps.Buffer); err != nil {
		return nil, ErrBadConn
	}
	if withHeader {
		//the last byte
		ps.NextPkgState = ps.Buffer[length+3]
		//next package length
		ps.NextPkgLen = uint32(uint32(ps.Buffer[length-1]) |
			uint32(ps.Buffer[length])<<8 |
			uint32(ps.Buffer[length+1])<<16)
	}

	return ps, nil
}

func (p *PacketIO) ReadPacket() ([]byte, error) {
	header := []byte{0, 0, 0, 0}

	if _, err := io.ReadFull(p.rb, header); err != nil {
		return nil, ErrBadConn
	}

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	if length < 1 {
		return nil, fmt.Errorf("invalid payload length %d", length)
	}

	sequence := uint8(header[3])

	if sequence != p.Sequence {
		return nil, fmt.Errorf("invalid sequence %d != %d", sequence, p.Sequence)
	}

	p.Sequence++

	data := make([]byte, length)
	if _, err := io.ReadFull(p.rb, data); err != nil {
		return nil, ErrBadConn
	} else {
		if length < MaxPayloadLen {
			return data, nil
		}

		var buf []byte
		buf, err = p.ReadPacket()
		if err != nil {
			return nil, ErrBadConn
		} else {
			return append(data, buf...), nil
		}
	}
}

//data already have header
func (p *PacketIO) WritePacket(data []byte) error {
	length := len(data) - 4

	for length >= MaxPayloadLen {

		data[0] = 0xff
		data[1] = 0xff
		data[2] = 0xff

		data[3] = p.Sequence

		if n, err := p.wb.Write(data[:4+MaxPayloadLen]); err != nil {
			return ErrBadConn
		} else if n != (4 + MaxPayloadLen) {
			return ErrBadConn
		} else {
			p.Sequence++
			length -= MaxPayloadLen
			data = data[MaxPayloadLen:]
		}
	}

	data[0] = byte(length)
	data[1] = byte(length >> 8)
	data[2] = byte(length >> 16)
	data[3] = p.Sequence

	if n, err := p.wb.Write(data); err != nil {
		return ErrBadConn
	} else if n != len(data) {
		return ErrBadConn
	} else {
		p.Sequence++
		return nil
	}
}

func (p *PacketIO) WritePacketBatch(total, data []byte, direct bool) ([]byte, error) {
	if data == nil {
		//only flush the buffer
		if direct == true {
			n, err := p.wb.Write(total)
			if err != nil {
				return nil, ErrBadConn
			}
			if n != len(total) {
				return nil, ErrBadConn
			}
		}
		return total, nil
	}

	length := len(data) - 4
	for length >= MaxPayloadLen {

		data[0] = 0xff
		data[1] = 0xff
		data[2] = 0xff

		data[3] = p.Sequence
		total = append(total, data[:4+MaxPayloadLen]...)

		p.Sequence++
		length -= MaxPayloadLen
		data = data[MaxPayloadLen:]
	}

	data[0] = byte(length)
	data[1] = byte(length >> 8)
	data[2] = byte(length >> 16)
	data[3] = p.Sequence

	total = append(total, data...)
	p.Sequence++

	if direct {
		if n, err := p.wb.Write(total); err != nil {
			return nil, ErrBadConn
		} else if n != len(total) {
			return nil, ErrBadConn
		}
	}
	return total, nil
}
