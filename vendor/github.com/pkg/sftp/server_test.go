package sftp

import (
	"io"
	"sync"
	"testing"
	"time"
)

func clientServerPair(t *testing.T) (*Client, *Server) {
	cr, sw := io.Pipe()
	sr, cw := io.Pipe()
	server, err := NewServer(struct {
		io.Reader
		io.WriteCloser
	}{sr, sw})
	if err != nil {
		t.Fatal(err)
	}
	go server.Serve()
	client, err := NewClientPipe(cr, cw)
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	return client, server
}

type sshFxpTestBadExtendedPacket struct {
	ID        uint32
	Extension string
	Data      string
}

func (p sshFxpTestBadExtendedPacket) id() uint32 { return p.ID }

func (p sshFxpTestBadExtendedPacket) MarshalBinary() ([]byte, error) {
	l := 1 + 4 + 4 + // type(byte) + uint32 + uint32
		len(p.Extension) +
		len(p.Data)

	b := make([]byte, 0, l)
	b = append(b, ssh_FXP_EXTENDED)
	b = marshalUint32(b, p.ID)
	b = marshalString(b, p.Extension)
	b = marshalString(b, p.Data)
	return b, nil
}

// test that errors are sent back when we request an invalid extended packet operation
func TestInvalidExtendedPacket(t *testing.T) {
	client, server := clientServerPair(t)
	defer client.Close()
	defer server.Close()

	badPacket := sshFxpTestBadExtendedPacket{client.nextID(), "thisDoesn'tExist", "foobar"}
	_, _, err := client.clientConn.sendPacket(badPacket)
	if err == nil {
		t.Fatal("expected error from bad packet")
	}

	// try to stat a file; the client should have shut down.
	filePath := "/etc/passwd"
	_, err = client.Stat(filePath)
	if err == nil {
		t.Fatal("expected error from closed connection")
	}

}

// Test what happens when the pool processes a close packet on a file that it
// is still reading from.
func TestCloseOutOfOrder(t *testing.T) {
	packets := []requestPacket{
		&sshFxpRemovePacket{ID: 0, Filename: "foo"},
		&sshFxpOpenPacket{ID: 1},
		&sshFxpWritePacket{ID: 2, Handle: "foo"},
		&sshFxpWritePacket{ID: 3, Handle: "foo"},
		&sshFxpWritePacket{ID: 4, Handle: "foo"},
		&sshFxpWritePacket{ID: 5, Handle: "foo"},
		&sshFxpClosePacket{ID: 6, Handle: "foo"},
		&sshFxpRemovePacket{ID: 7, Filename: "foo"},
	}

	recvChan := make(chan requestPacket, len(packets)+1)
	sender := newsender()
	pm := newPktMgr(sender)
	svr := Server{pktMgr: pm}
	wg := sync.WaitGroup{}
	wg.Add(len(packets))
	worker := func(ch requestChan) {
		for pkt := range ch {
			if _, ok := pkt.(*sshFxpWritePacket); ok {
				// sleep to cause writes to come after close/remove
				time.Sleep(time.Millisecond)
			}
			pm.working.Done()
			recvChan <- pkt
			wg.Done()
		}
	}
	pktChan := svr.sftpServerWorkers(worker)
	for _, p := range packets {
		pktChan <- p
	}
	wg.Wait()
	close(recvChan)
	received := []requestPacket{}
	for p := range recvChan {
		received = append(received, p)
	}
	if received[len(received)-2].id() != packets[len(packets)-2].id() {
		t.Fatal("Packets processed out of order.")
	}
	if received[len(received)-1].id() != packets[len(packets)-1].id() {
		t.Fatal("Packets processed out of order.")
	}
}

// test that server handles concurrent requests correctly
func TestConcurrentRequests(t *testing.T) {
	client, server := clientServerPair(t)
	defer client.Close()
	defer server.Close()

	concurrency := 2
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < 1024; j++ {
				f, err := client.Open("/etc/passwd")
				if err != nil {
					t.Errorf("failed to open file: %v", err)
				}
				if err := f.Close(); err != nil {
					t.Errorf("failed t close file: %v", err)
				}
			}
		}()
	}
	wg.Wait()
}
