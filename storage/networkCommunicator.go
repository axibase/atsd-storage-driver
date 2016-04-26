/*
* Copyright 2015 Axibase Corporation or its affiliates. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
* https://www.axibase.com/atsd/axibase-apache-2.0.pdf
*
* or in the "license" file accompanying this file. This file is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License.
 */

package storage

import (
	"errors"
	"fmt"
	"github.com/axibase/atsd-api-go/net"
	"github.com/golang/glog"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"
)

const (
	seriesCommandsChunkChannelBufferSize = 1000
	bufferSize                           = 16384
)

type counters struct {
	series, entityTag, prop, messages struct{ sent, dropped uint64 }
}

type NetworkCommunicator struct {
	seriesCommandsChunkChan chan *Chunk
	properties              chan []*net.PropertyCommand
	messageCommands         chan []*net.MessageCommand
	entityTag               chan []*net.EntityTagCommand

	protocol string
	hostport string

	counters []*counters

	goroutinesCount int
}

func NewNetworkCommunicator(goroutineCount int, url *url.URL) (*NetworkCommunicator, error) {
	if goroutineCount <= 0 {
		return nil, errors.New(fmt.Sprintf("goroutines_count should be > 0, provided value = %v", goroutineCount))
	}
	if url.Scheme != "tcp" && url.Scheme != "udp" {
		return nil, errors.New(fmt.Sprintf("unsupported protocol: %v", url.Scheme))
	}

	nc := &NetworkCommunicator{
		protocol:                url.Scheme,
		hostport:                url.Host,
		goroutinesCount:         goroutineCount,
		seriesCommandsChunkChan: make(chan *Chunk, seriesCommandsChunkChannelBufferSize),
		properties:              make(chan []*net.PropertyCommand),
		messageCommands:         make(chan []*net.MessageCommand),
		entityTag:               make(chan []*net.EntityTagCommand),
		counters:                make([]*counters, goroutineCount, goroutineCount),
	}

	for i := 0; i < goroutineCount; i++ {
		nc.counters[i] = &counters{}
	}

	for i := 0; i < goroutineCount; i++ {
		go func(threadNum int, counters *counters) {
			expBackoff := NewExpBackoff(100*time.Millisecond, 5*time.Minute)
		start:
			for {
				conn, err := net.DialTimeout(nc.protocol, nc.hostport, 5*time.Second, bufferSize)
				if err != nil {
					waitDuration := expBackoff.Duration()
					glog.Error("Thread ", threadNum, " could not init connection, waiting for ", waitDuration, " err: ", err)
					time.Sleep(waitDuration)
					continue
				}
				expBackoff.Reset()
				for {
					select {
					case entityTag := <-nc.entityTag:
						for i := range entityTag {
							err := conn.EntityTag(entityTag[i])
							if err != nil {
								glog.Error("Thread ", threadNum, " could not send entity update command: ", err)
								conn.Close()
								atomic.AddUint64(&counters.entityTag.dropped, uint64(len(entityTag)-i))
								continue start
							}
							atomic.AddUint64(&counters.entityTag.sent, 1)
						}
					case properties := <-nc.properties:
						for i := range properties {
							err := conn.Property(properties[i])
							if err != nil {
								glog.Error("Thread ", threadNum, " could not send property command: ", err)
								conn.Close()
								atomic.AddUint64(&counters.prop.dropped, uint64(len(properties)-i))
								continue start
							}
							atomic.AddUint64(&counters.prop.sent, 1)
						}
					case messageCommands := <-nc.messageCommands:
						for i := range messageCommands {
							err := conn.Message(messageCommands[i])
							if err != nil {
								glog.Error("Thread ", threadNum, " could not send message command: ", err)
								conn.Close()
								atomic.AddUint64(&counters.messages.dropped, uint64(len(messageCommands)-i))
								continue start
							}
							atomic.AddUint64(&counters.messages.sent, 1)
						}
					case seriesChunk := <-nc.seriesCommandsChunkChan:
						for el := seriesChunk.Front(); el != nil; el = seriesChunk.Front() {
							err := conn.Series(el.Value.(*net.SeriesCommand))
							if err != nil {
								glog.Error("Thread ", threadNum, " could not send series command: ", err)
								conn.Close()
								atomic.AddUint64(&counters.series.dropped, uint64(seriesChunk.Len()))
								continue start
							}
							seriesChunk.Remove(el)
							atomic.AddUint64(&counters.series.sent, 1)
						}
					}
					conn.Flush()
				}
			}
		}(i, nc.counters[i])
	}

	return nc, nil
}

func (self *NetworkCommunicator) QueuedSendData(seriesCommandsChunk []*Chunk, entityTagCommands []*net.EntityTagCommand, properties []*net.PropertyCommand, messageCommands []*net.MessageCommand) {
	self.entityTag <- entityTagCommands

	self.properties <- properties

	self.messageCommands <- messageCommands

	for _, val := range seriesCommandsChunk {
		self.seriesCommandsChunkChan <- val
	}
}

func (self *NetworkCommunicator) PriorSendData(seriesCommands []*net.SeriesCommand, entityTagCommands []*net.EntityTagCommand, propertyCommands []*net.PropertyCommand, messageCommands []*net.MessageCommand) {
	conn, err := net.DialTimeout(self.protocol, self.hostport, 1*time.Second, 1)
	if err != nil {
		glog.Error("Could not init connection to prior send self metrics", err)
		return
	}
	for i := range entityTagCommands {
		err = conn.EntityTag(entityTagCommands[i])
		if err != nil {
			glog.Error("Could not prior send entity-tag command", err)
		}
	}
	for i := range propertyCommands {
		err = conn.Property(propertyCommands[i])
		if err != nil {
			glog.Error("Could not prior send property command", err)
		}
	}
	for i := range seriesCommands {
		err = conn.Series(seriesCommands[i])
		if err != nil {
			glog.Error("Could not prior send series command", err)
		}
	}
	for i := range messageCommands {
		err = conn.Message(messageCommands[i])
		if err != nil {
			glog.Error("Could not prior send message command", err)
		}
	}

	conn.Flush()
	conn.Close()
}

func (self *NetworkCommunicator) SelfMetricValues() []*metricValue {
	metricValues := []*metricValue{}
	for i := range self.counters {
		metricValues = append(metricValues,
			&metricValue{
				name: "series-commands.sent",
				tags: map[string]string{
					"thread":    strconv.FormatInt(int64(i), 10),
					"transport": self.protocol,
				},
				value: net.Int64(atomic.LoadUint64(&self.counters[i].series.sent)),
			},
			&metricValue{
				name: "series-commands.dropped",
				tags: map[string]string{
					"thread":    strconv.FormatInt(int64(i), 10),
					"transport": self.protocol,
				},
				value: net.Int64(atomic.LoadUint64(&self.counters[i].series.dropped)),
			},
			&metricValue{
				name: "message-commands.sent",
				tags: map[string]string{
					"thread":    strconv.FormatInt(int64(i), 10),
					"transport": self.protocol,
				},
				value: net.Int64(atomic.LoadUint64(&self.counters[i].messages.sent)),
			},
			&metricValue{
				name: "message-commands.dropped",
				tags: map[string]string{
					"thread":    strconv.FormatInt(int64(i), 10),
					"transport": self.protocol,
				},
				value: net.Int64(atomic.LoadUint64(&self.counters[i].messages.dropped)),
			},
			&metricValue{
				name: "property-commands.sent",
				tags: map[string]string{
					"thread":    strconv.FormatInt(int64(i), 10),
					"transport": self.protocol,
				},
				value: net.Int64(atomic.LoadUint64(&self.counters[i].prop.sent)),
			},
			&metricValue{
				name: "property-commands.dropped",
				tags: map[string]string{
					"thread":    strconv.FormatInt(int64(i), 10),
					"transport": self.protocol,
				},
				value: net.Int64(atomic.LoadUint64(&self.counters[i].prop.dropped)),
			},
			&metricValue{
				name: "entitytag-commands.sent",
				tags: map[string]string{
					"thread":    strconv.FormatInt(int64(i), 10),
					"transport": self.protocol,
				},
				value: net.Int64(atomic.LoadUint64(&self.counters[i].entityTag.sent)),
			},
			&metricValue{
				name: "entitytag-commands.dropped",
				tags: map[string]string{
					"thread":    strconv.FormatInt(int64(i), 10),
					"transport": self.protocol,
				},
				value: net.Int64(atomic.LoadUint64(&self.counters[i].entityTag.dropped)),
			},
		)
	}
	return metricValues
}
