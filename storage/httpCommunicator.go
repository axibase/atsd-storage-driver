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
	"github.com/axibase/atsd-api-go/net/http"
	httpmodel "github.com/axibase/atsd-api-go/net/http/model"
	netmodel "github.com/axibase/atsd-api-go/net/model"
	"github.com/golang/glog"
	"time"
)

type HttpCommunicator struct {
	client *http.Client

	seriesCommandsChunkChan chan *Chunk
	propertyCommands        chan []*netmodel.PropertyCommand
	entityTag               chan []*netmodel.EntityTagCommand
	messageCommands         chan []*netmodel.MessageCommand
	counters                *httpCounters
}

type httpCounters struct {
	series, entityTag, prop, messages struct{ sent, dropped uint64 }
}

func NewHttpCommunicator(client *http.Client) *HttpCommunicator {
	hc := &HttpCommunicator{
		client:                  client,
		seriesCommandsChunkChan: make(chan *Chunk),
		propertyCommands:        make(chan []*netmodel.PropertyCommand),
		entityTag:               make(chan []*netmodel.EntityTagCommand),
		messageCommands:         make(chan []*netmodel.MessageCommand),
		counters:                &httpCounters{},
	}
	go func() {
		for {
			expBackoff := NewExpBackoff(100*time.Millisecond, 5*time.Minute)
			select {
			case entityTag := <-hc.entityTag:
				entities := entityTagCommandsToEntities(entityTag)
				for _, entity := range entities {
					err := hc.client.Entities.Update(entity)
					if err != nil {
						err = hc.client.Entities.Create(entity)
						if err != nil {
							waitDuration := expBackoff.Duration()
							glog.Error("Could not send entity update: ", err)
							time.Sleep(waitDuration)
							hc.counters.entityTag.dropped++
							continue
						}
					}
					hc.counters.entityTag.sent++
				}

			case propertyCommands := <-hc.propertyCommands:
				if len(propertyCommands) > 0 {
					properties := propertyCommandsToProperties(propertyCommands)
					err := hc.client.Properties.Insert(properties)
					if err != nil {
						waitDuration := expBackoff.Duration()
						glog.Error("Could not send property: ", err)
						time.Sleep(waitDuration)
						hc.counters.prop.dropped += uint64(len(properties))
						continue
					}
					hc.counters.prop.sent += uint64(len(properties))
				}
			case messageCommands := <-hc.messageCommands:
				if len(messageCommands) > 0 {
					messages := messageCommandsToProperties(messageCommands)
					err := hc.client.Messages.Insert(messages)
					if err != nil {
						waitDuration := expBackoff.Duration()
						glog.Error("Could not send message: ", err)
						time.Sleep(waitDuration)
						hc.counters.messages.dropped += uint64(len(messages))
						continue
					}
					hc.counters.messages.sent += uint64(len(messages))
				}

			case seriesChunk := <-hc.seriesCommandsChunkChan:
				series := seriesCommandsChunkToSeries(seriesChunk)
				if len(series) > 0 {
					err := hc.client.Series.Insert(series)
					if err != nil {
						waitDuration := expBackoff.Duration()
						glog.Error("Could not send series: ", err)
						time.Sleep(waitDuration)
						hc.counters.series.dropped += uint64(len(series))
						continue
					}
					hc.counters.series.sent += uint64(len(series))
				}
			}
			expBackoff.Reset()
		}
	}()

	return hc
}

func (self *HttpCommunicator) QueuedSendData(seriesCommandsChunk []*Chunk, entityTagCommands []*netmodel.EntityTagCommand, propertyCommands []*netmodel.PropertyCommand, messageCommands []*netmodel.MessageCommand) {
	self.propertyCommands <- propertyCommands

	self.entityTag <- entityTagCommands

	self.messageCommands <- messageCommands

	for _, val := range seriesCommandsChunk {
		self.seriesCommandsChunkChan <- val
	}
}

func (self *HttpCommunicator) PriorSendData(seriesCommands []*netmodel.SeriesCommand, entityTagCommands []*netmodel.EntityTagCommand, propertyCommands []*netmodel.PropertyCommand, messageCommands []*netmodel.MessageCommand) {
	entities := entityTagCommandsToEntities(entityTagCommands)
	for _, entity := range entities {
		err := self.client.Entities.Update(entity)
		if err != nil {
			err = self.client.Entities.Create(entity)
			if err != nil {
				glog.Error("Could not prior send entity update: ", err)
			}
		}
	}
	properties := propertyCommandsToProperties(propertyCommands)
	err := self.client.Properties.Insert(properties)
	if err != nil {
		glog.Error("Could not prior send property: ", err)
	}
	series := seriesCommandsToSeries(seriesCommands)
	err = self.client.Series.Insert(series)
	if err != nil {
		glog.Error("Could not prior send series: ", err)
	}
	messages := messageCommandsToProperties(messageCommands)
	err = self.client.Messages.Insert(messages)
	if err != nil {
		glog.Error("Could not prior send message: ", err)
	}
}
func (self *HttpCommunicator) SelfMetricValues() []*metricValue {
	return []*metricValue{
		&metricValue{
			name: "series-commands.sent",
			tags: map[string]string{
				"transport": self.client.Url().Scheme,
			},
			value: netmodel.Int64(self.counters.series.sent),
		},
		&metricValue{
			name: "series-commands.dropped",
			tags: map[string]string{
				"transport": self.client.Url().Scheme,
			},
			value: netmodel.Int64(self.counters.series.dropped),
		},
		&metricValue{
			name: "message-commands.sent",
			tags: map[string]string{
				"transport": self.client.Url().Scheme,
			},
			value: netmodel.Int64(self.counters.messages.sent),
		},
		&metricValue{
			name: "message-commands.dropped",
			tags: map[string]string{
				"transport": self.client.Url().Scheme,
			},
			value: netmodel.Int64(self.counters.messages.dropped),
		},
		&metricValue{
			name: "property-commands.sent",
			tags: map[string]string{
				"transport": self.client.Url().Scheme,
			},
			value: netmodel.Int64(self.counters.prop.sent),
		},
		&metricValue{
			name: "property-commands.dropped",
			tags: map[string]string{
				"transport": self.client.Url().Scheme,
			},
			value: netmodel.Int64(self.counters.prop.dropped),
		},
		&metricValue{
			name: "entitytag-commands.sent",
			tags: map[string]string{
				"transport": self.client.Url().Scheme,
			},
			value: netmodel.Int64(self.counters.entityTag.sent),
		},
		&metricValue{
			name: "entitytag-commands.dropped",
			tags: map[string]string{
				"transport": self.client.Url().Scheme,
			},
			value: netmodel.Int64(self.counters.entityTag.dropped),
		},
	}
}

func seriesCommandsToSeries(seriesCommands []*netmodel.SeriesCommand) []*httpmodel.Series {
	series := []*httpmodel.Series{}

	for _, command := range seriesCommands {
		metrics := command.Metrics()
		timestamp := command.Timestamp()
		if timestamp == nil {
			panic("Nil timestamp!")
		}
		tags := command.Tags()
		for key, val := range metrics {
			series = append(series, httpmodel.NewSeries(command.Entity(), key).AddSample(*timestamp, val).SetTags(tags))
		}
	}
	return series
}
func seriesCommandsChunkToSeries(seriesCommandsChunk *Chunk) []*httpmodel.Series {
	series := []*httpmodel.Series{}
	if seriesCommandsChunk.Len() > 0 {
		seriesMap := map[string]*httpmodel.Series{}
		for el := seriesCommandsChunk.Front(); el != nil; el = seriesCommandsChunk.Front() {
			seriesCommand := *el.Value.(*netmodel.SeriesCommand)
			metrics := seriesCommand.Metrics()
			tags := seriesCommand.Tags()
			for key, val := range metrics {
				if _, ok := seriesMap[key]; !ok {
					seriesMap[key] = httpmodel.NewSeries(seriesCommand.Entity(), key).SetTags(tags)
				}
				if seriesCommand.Timestamp() == nil {
					panic("Nil timestamp!")
				}
				seriesMap[key].AddSample(*seriesCommand.Timestamp(), val)
			}
			seriesCommandsChunk.Remove(el)
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
		if propertyCommand.Timestamp() != nil {
			property.SetTimestamp(*propertyCommand.Timestamp())
		}

		properties = append(properties, property)
	}
	return properties
}
func messageCommandsToProperties(messageCommands []*netmodel.MessageCommand) []*httpmodel.Message {
	messages := []*httpmodel.Message{}
	for _, messageCommand := range messageCommands {
		message := httpmodel.NewMessage(messageCommand.Entity()).
			SetMessage(messageCommand.Message())
		for key, val := range messageCommand.Tags() {
			if key == "severity" {
				message.SetSeverity(httpmodel.Severity(val))
			}
			if key == "source" {
				message.SetSource(val)
			}
			if key == "type" {
				message.SetType(val)
			}
			message.SetTag(key, val)
		}
		if messageCommand.Timestamp() != nil {
			message.SetTimestamp(*messageCommand.Timestamp())
		}

		messages = append(messages, message)
	}
	return messages
}