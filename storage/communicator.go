// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"container/list"
	"github.com/golang/glog"
	"github.com/axibase/atsd-api-go/net"
	"github.com/axibase/atsd-api-go/net/http"
	httpmodel "github.com/axibase/atsd-api-go/net/http/model"
	netmodel "github.com/axibase/atsd-api-go/net/model"
	"strconv"
	"time"
)

const (
	seriesCommandsChunkChannelBufferSize = 1000
	bufferSize                           = 16384
)

const (
	selfMetricPrefix = "storagedriver.cadvisor"
)

type WriteCommunicator interface {
	SendData(seriesCommandsChunk []*list.List, entityTagCommands []*netmodel.EntityTagCommand, properties []*netmodel.PropertyCommand)
}

type counters struct {
	series, entityTag, prop struct{ sent, dropped uint64 }
}

type NetworkCommunicator struct {
	seriesCommandsChunkChan chan *list.List
	properties              chan []*netmodel.PropertyCommand
	entityTag               chan []*netmodel.EntityTagCommand

	counters []*counters

	connectionLimit int
}

func NewNetworkCommunicator(connectionLimit int, protocol, hostport, hostname string) *NetworkCommunicator {
	nc := &NetworkCommunicator{
		connectionLimit:         connectionLimit,
		seriesCommandsChunkChan: make(chan *list.List, seriesCommandsChunkChannelBufferSize),
		properties:              make(chan []*netmodel.PropertyCommand),
		entityTag:               make(chan []*netmodel.EntityTagCommand),
		counters:                make([]*counters, connectionLimit, connectionLimit),
	}

	for i := 0; i < connectionLimit; i++ {
		nc.counters[i] = &counters{}
	}

	for i := 0; i < connectionLimit; i++ {
		go func(threadNum int, counters *counters) {
			expBackoff := NewExpBackoff(100*time.Millisecond, 5*time.Minute)
		start:
			for {
				conn, err := net.DialTimeout(protocol, hostport, 5*time.Second, bufferSize)
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
								counters.entityTag.dropped += uint64(len(entityTag) - i)
								continue start
							}
							counters.entityTag.sent++
						}
					case properties := <-nc.properties:
						for i := range properties {
							err := conn.Property(properties[i])
							if err != nil {
								glog.Error("Thread ", threadNum, " could not send property command: ", err)
								conn.Close()
								counters.prop.dropped += uint64(len(properties) - i)
								continue start
							}
							counters.prop.sent++
						}
					case seriesList := <-nc.seriesCommandsChunkChan:
						for el := seriesList.Front(); el != nil; el = seriesList.Front() {
							err := conn.Series(el.Value.(*netmodel.SeriesCommand))
							if err != nil {
								glog.Error("Thread ", threadNum, " could not send series command: ", err)
								conn.Close()
								counters.series.dropped += uint64(seriesList.Len())
								continue start
							}
							seriesList.Remove(el)
							counters.series.sent++
						}
					}
					conn.Flush()
				}
			}
		}(i, nc.counters[i])
	}
	metricSend := func() {
		currentMillis := uint64(time.Now().UnixNano() / 1e6)
		seriesCommands := []*netmodel.SeriesCommand{}
		for i := range nc.counters {
			seriesCommands = append(seriesCommands,
				netmodel.NewSeriesCommand(hostname, selfMetricPrefix+".series-commands.sent", float64(nc.counters[i].series.sent)).
					SetTag("thread", strconv.FormatInt(int64(i), 10)).
					SetMetricValue(selfMetricPrefix+".series-commands.dropped", float64(nc.counters[i].series.dropped)).
					SetMetricValue(selfMetricPrefix+".property-commands.sent", float64(nc.counters[i].prop.sent)).
					SetMetricValue(selfMetricPrefix+".property-commands.dropped", float64(nc.counters[i].prop.dropped)).
					SetMetricValue(selfMetricPrefix+".entitytag-commands.sent", float64(nc.counters[i].entityTag.sent)).
					SetMetricValue(selfMetricPrefix+".entitytag-commands.dropped", float64(nc.counters[i].entityTag.dropped)).
					SetTimestamp(currentMillis),
			)
		}
		conn, err := net.DialTimeout(protocol, hostport, 1*time.Second, 200)
		if err != nil {
			glog.Error("Could not init connection to send self metrics", err)
			return
		}
		for i := range seriesCommands {
			err = conn.Series(seriesCommands[i])
			if err != nil {
				glog.Error("Could not send series command", err)
			}
		}
		conn.Flush()
	}

	schedule(metricSend, 15*time.Second)

	return nc
}

func (self *NetworkCommunicator) SendData(seriesCommandsChunk []*list.List, entityTagCommands []*netmodel.EntityTagCommand, properties []*netmodel.PropertyCommand) {
	self.entityTag <- entityTagCommands

	self.properties <- properties

	for _, val := range seriesCommandsChunk {
		self.seriesCommandsChunkChan <- val
	}
}

type HttpCommunicator struct {
	url      string
	username string
	password string

	seriesCommandsChunkChan chan *list.List
	propertyCommands        chan []*netmodel.PropertyCommand
	entityTag               chan []*netmodel.EntityTagCommand
}

func NewHttpCommunicator(url, username, password string) *HttpCommunicator {
	hc := &HttpCommunicator{
		url:      url,
		username: username,
		password: password,

		seriesCommandsChunkChan: make(chan *list.List),
		propertyCommands:        make(chan []*netmodel.PropertyCommand),
		entityTag:               make(chan []*netmodel.EntityTagCommand),
	}

	go func() {
	start:
		for {
			client := http.New(hc.url, hc.username, hc.password)
			for {
				select {
				case entityTag := <-hc.entityTag:
					entities := entityTagCommandsToEntities(entityTag)
					for _, entity := range entities {
						err := client.Entities.Update(entity)
						if err != nil {
							err = client.Entities.Create(entity)
							if err != nil {
								glog.Error("Could not send entity update: ", err)
								continue start
							}
						}
					}

				case propertyCommands := <-hc.propertyCommands:
					properties := propertyCommandsToProperties(propertyCommands)
					err := client.Properties.Insert(properties)
					if err != nil {
						glog.Error("Could not send property: ", err)
						continue start
					}

				case seriesList := <-hc.seriesCommandsChunkChan:
					series := seriesCommandsListToSeries(seriesList)
					err := client.Series.Insert(series)
					if err != nil {
						glog.Error("Could not send series: ", err)
						continue start
					}
				}
			}
		}
	}()

	return hc
}

func (self *HttpCommunicator) SendData(seriesCommandsChunk []*list.List, entityTagCommands []*netmodel.EntityTagCommand, propertyCommands []*netmodel.PropertyCommand) {
	self.propertyCommands <- propertyCommands

	self.entityTag <- entityTagCommands

	for _, val := range seriesCommandsChunk {
		self.seriesCommandsChunkChan <- val
	}
}

func seriesCommandsToSeries(seriesCommands []*netmodel.SeriesCommand) []*httpmodel.Series {
	series := []*httpmodel.Series{}

	for _, command := range seriesCommands {
		metrics := command.Metrics()
		timestamp := command.Timestamp() //command always has timestamp
		tags := command.Tags()
		for key, val := range metrics {
			series = append(series, httpmodel.NewSeries(command.Entity(), key).AddSample(timestamp, val).AddTags(tags))
		}
	}
	return series
}
func seriesCommandsListToSeries(seriesCommandsList *list.List) []*httpmodel.Series {
	series := []*httpmodel.Series{}
	if seriesCommandsList.Len() > 0 {
		seriesMap := map[string]*httpmodel.Series{}
		for el := seriesCommandsList.Front(); el != nil; el = seriesCommandsList.Front() {
			seriesCommand := *el.Value.(*netmodel.SeriesCommand)
			metrics := seriesCommand.Metrics()
			tags := seriesCommand.Tags()
			for key, val := range metrics {
				if _, ok := seriesMap[key]; !ok {
					seriesMap[key] = httpmodel.NewSeries(seriesCommand.Entity(), key).AddTags(tags)
				}
				seriesMap[key].AddSample(seriesCommand.Timestamp(), val)
			}
			seriesCommandsList.Remove(el)
		}
		for _, s := range seriesMap {
			series = append(series, s)
		}
	}
	return series
}
func entityTagCommandsToEntities(entityTagCommands []*netmodel.EntityTagCommand) []*httpmodel.Entity {
	entities := []*httpmodel.Entity{}

	for _, command := range entityTagCommands {
		entity := httpmodel.NewEntity(command.Entity())
		for key, value := range command.Tags() {
			entity.SetTag(key, value)
		}
		entities = append(entities, entity)
	}
	return entities
}
func propertyCommandsToProperties(propertyCommands []*netmodel.PropertyCommand) []*httpmodel.Property {
	properties := []*httpmodel.Property{}
	for _, propertyCommand := range propertyCommands {
		property := httpmodel.NewProperty(propertyCommand.PropType(), propertyCommand.Entity()).
			SetKey(propertyCommand.Key()).
			SetAllTags(propertyCommand.Tags())
		if propertyCommand.HasTimestamp() {
			property.SetTimestamp(propertyCommand.Timestamp())
		}

		properties = append(properties, property)
	}
	return properties
}
