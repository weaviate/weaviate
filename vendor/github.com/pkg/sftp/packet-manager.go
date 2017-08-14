package sftp

import (
	"encoding"
	"sync"
)

// The goal of the packetManager is to keep the outgoing packets in the same
// order as the incoming. This is due to some sftp clients requiring this
// behavior (eg. winscp).

type packetSender interface {
	sendPacket(encoding.BinaryMarshaler) error
}

type packetManager struct {
	requests  chan requestPacket
	responses chan responsePacket
	fini      chan struct{}
	incoming  requestPacketIDs
	outgoing  responsePackets
	sender    packetSender // connection object
	working   *sync.WaitGroup
}

func newPktMgr(sender packetSender) packetManager {
	s := packetManager{
		requests:  make(chan requestPacket, sftpServerWorkerCount),
		responses: make(chan responsePacket, sftpServerWorkerCount),
		fini:      make(chan struct{}),
		incoming:  make([]uint32, 0, sftpServerWorkerCount),
		outgoing:  make([]responsePacket, 0, sftpServerWorkerCount),
		sender:    sender,
		working:   &sync.WaitGroup{},
	}
	go s.worker()
	return s
}

// register incoming packets to be handled
// send id of 0 for packets without id
func (s packetManager) incomingPacket(pkt requestPacket) {
	s.working.Add(1)
	s.requests <- pkt // buffer == sftpServerWorkerCount
}

// register outgoing packets as being ready
func (s packetManager) readyPacket(pkt responsePacket) {
	s.responses <- pkt
	s.working.Done()
}

// shut down packetManager worker
func (s packetManager) close() {
	close(s.fini)
}

// process packets
func (s *packetManager) worker() {
	for {
		select {
		case pkt := <-s.requests:
			debug("incoming id: %v", pkt.id())
			s.incoming = append(s.incoming, pkt.id())
			if len(s.incoming) > 1 {
				s.incoming.Sort()
			}
		case pkt := <-s.responses:
			debug("outgoing pkt: %v", pkt.id())
			s.outgoing = append(s.outgoing, pkt)
			if len(s.outgoing) > 1 {
				s.outgoing.Sort()
			}
		case <-s.fini:
			return
		}
		s.maybeSendPackets()
	}
}

// send as many packets as are ready
func (s *packetManager) maybeSendPackets() {
	for {
		if len(s.outgoing) == 0 || len(s.incoming) == 0 {
			debug("break! -- outgoing: %v; incoming: %v",
				len(s.outgoing), len(s.incoming))
			break
		}
		out := s.outgoing[0]
		in := s.incoming[0]
		// 		debug("incoming: %v", s.incoming)
		// 		debug("outgoing: %v", outfilter(s.outgoing))
		if in == out.id() {
			s.sender.sendPacket(out)
			// pop off heads
			copy(s.incoming, s.incoming[1:])            // shift left
			s.incoming = s.incoming[:len(s.incoming)-1] // remove last
			copy(s.outgoing, s.outgoing[1:])            // shift left
			s.outgoing = s.outgoing[:len(s.outgoing)-1] // remove last
		} else {
			break
		}
	}
}

func outfilter(o []responsePacket) []uint32 {
	res := make([]uint32, 0, len(o))
	for _, v := range o {
		res = append(res, v.id())
	}
	return res
}
