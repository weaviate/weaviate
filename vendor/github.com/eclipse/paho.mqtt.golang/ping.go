/*
 * Copyright (c) 2013 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *    Seth Hoenig
 *    Allan Stockdill-Mander
 *    Mike Robertson
 */

package mqtt

import (
	"errors"
	"sync"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

func keepalive(c *client) {
	DEBUG.Println(PNG, "keepalive starting")

	var condWG sync.WaitGroup
	pingStop := make(chan struct{})

	defer func() {
		close(pingStop)
		c.keepaliveReset.Broadcast()
		c.pingResp.Broadcast()
		c.packetResp.Broadcast()
		condWG.Wait()
		c.workers.Done()
	}()

	receiveInterval := c.options.KeepAlive + (1 * time.Second)
	pingTimer := timer{Timer: time.NewTimer(c.options.KeepAlive)}
	receiveTimer := timer{Timer: time.NewTimer(receiveInterval)}
	pingRespTimer := timer{Timer: time.NewTimer(c.options.PingTimeout)}

	pingRespTimer.Stop()

	condWG.Add(3)
	go func() {
		defer condWG.Done()
		for {
			c.pingResp.L.Lock()
			c.pingResp.Wait()
			c.pingResp.L.Unlock()
			select {
			case <-pingStop:
				return
			default:
			}
			DEBUG.Println(NET, "resetting ping timeout timer")
			pingRespTimer.Stop()
			pingTimer.Reset(c.options.KeepAlive)
			receiveTimer.Reset(receiveInterval)
		}
	}()

	go func() {
		defer condWG.Done()
		for {
			c.packetResp.L.Lock()
			c.packetResp.Wait()
			c.packetResp.L.Unlock()
			select {
			case <-pingStop:
				return
			default:
			}
			DEBUG.Println(NET, "resetting receive timer")
			receiveTimer.Reset(receiveInterval)
		}
	}()

	go func() {
		defer condWG.Done()
		for {
			c.keepaliveReset.L.Lock()
			c.keepaliveReset.Wait()
			c.keepaliveReset.L.Unlock()
			select {
			case <-pingStop:
				return
			default:
			}
			DEBUG.Println(NET, "resetting ping timer")
			pingTimer.Reset(c.options.KeepAlive)
		}
	}()

	for {
		select {
		case <-c.stop:
			DEBUG.Println(PNG, "keepalive stopped")
			return
		case <-pingTimer.C:
			sendPing(&pingTimer, &pingRespTimer, c)
		case <-receiveTimer.C:
			receiveTimer.SetRead(true)
			receiveTimer.Reset(receiveInterval)
			sendPing(&pingTimer, &pingRespTimer, c)
		case <-pingRespTimer.C:
			pingRespTimer.SetRead(true)
			CRITICAL.Println(PNG, "pingresp not received, disconnecting")
			c.errors <- errors.New("pingresp not received, disconnecting")
			pingTimer.Stop()
			return
		}
	}
}

type timer struct {
	sync.Mutex
	*time.Timer
	readFrom bool
}

func (t *timer) SetRead(v bool) {
	t.Lock()
	t.readFrom = v
	t.Unlock()
}

func (t *timer) Stop() bool {
	t.Lock()
	defer t.SetRead(true)
	defer t.Unlock()

	if !t.Timer.Stop() && !t.readFrom {
		<-t.C
		return false
	}
	return true
}

func (t *timer) Reset(d time.Duration) bool {
	t.Lock()
	defer t.SetRead(false)
	defer t.Unlock()
	if !t.Timer.Stop() && !t.readFrom {
		<-t.C
	}

	return t.Timer.Reset(d)
}

func sendPing(pt *timer, rt *timer, c *client) {
	pt.SetRead(true)
	DEBUG.Println(PNG, "keepalive sending ping")
	ping := packets.NewControlPacket(packets.Pingreq).(*packets.PingreqPacket)
	//We don't want to wait behind large messages being sent, the Write call
	//will block until it it able to send the packet.
	ping.Write(c.conn)

	rt.Reset(c.options.PingTimeout)
}
