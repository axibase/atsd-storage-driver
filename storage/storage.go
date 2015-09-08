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
	"container/list"
	"github.com/axibase/atsd-api-go/net/http"
	httpmodel "github.com/axibase/atsd-api-go/net/http/model"
	netmodel "github.com/axibase/atsd-api-go/net/model"
	"sync"
	"time"
)

type metricValue struct {
	name  string
	tags  map[string]string
	value netmodel.Number
}

type Chunk struct {
	*list.List
}

func NewChunk() *Chunk {
	return &Chunk{list.New()}
}

type IWriteCommunicator interface {
	QueuedSendData(seriesCommandsChunk []*Chunk, entityTagCommands []*netmodel.EntityTagCommand, properties []*netmodel.PropertyCommand, messages []*netmodel.MessageCommand)
	PriorSendData(seriesCommands []*netmodel.SeriesCommand, entityTagCommands []*netmodel.EntityTagCommand, propertyCommands []*netmodel.PropertyCommand, messageCommands []*netmodel.MessageCommand)
	SelfMetricValues() []*metricValue
}
type Storage struct {
	senderHostname string
	metricPrefix   string

	memstore          *MemStore
	writeCommunicator IWriteCommunicator

	atsdHttpClient *http.Client

	isUpdating     bool
	updateInterval time.Duration
	stopUpdateTask chan bool
	mutex          sync.Mutex
}

func (self *Storage) updateTask() {
	seriesCommandsChunks := self.memstore.ReleaseSeriesCommandChunks()
	properties := self.memstore.ReleaseProperties()
	entityTagCommands := self.memstore.ReleaseEntityTagCommands()
	messageCommands := self.memstore.ReleaseMessageCommands()

	self.writeCommunicator.QueuedSendData(seriesCommandsChunks, entityTagCommands, properties, messageCommands)

}
func (self *Storage) selfMetricSendTask() {
	timestamp := netmodel.Millis(time.Now().UnixNano() / 1e6)
	writeCommunicatorMetricValues := self.writeCommunicator.SelfMetricValues()

	seriesCommands := []*netmodel.SeriesCommand{}
	for _, metricValue := range writeCommunicatorMetricValues {
		seriesCommand := netmodel.NewSeriesCommand(self.senderHostname, self.metricPrefix+"."+metricValue.name, metricValue.value).
			SetTimestamp(timestamp)
		for name, val := range metricValue.tags {
			seriesCommand.SetTag(name, val)
		}
		seriesCommands = append(seriesCommands, seriesCommand)
	}

	seriesCommand := netmodel.NewSeriesCommand(self.senderHostname, self.metricPrefix+".memstore.entities.count", netmodel.Int64(self.memstore.EntitiesCount())).SetTimestamp(timestamp)
	seriesCommands = append(seriesCommands, seriesCommand)
	seriesCommand = netmodel.NewSeriesCommand(self.senderHostname, self.metricPrefix+".memstore.Messages.count", netmodel.Int64(self.memstore.MessagesCount())).SetTimestamp(timestamp)
	seriesCommands = append(seriesCommands, seriesCommand)
	seriesCommand = netmodel.NewSeriesCommand(self.senderHostname, self.metricPrefix+".memstore.properties.count", netmodel.Int64(self.memstore.PropertiesCount())).SetTimestamp(timestamp)
	seriesCommands = append(seriesCommands, seriesCommand)
	seriesCommand = netmodel.NewSeriesCommand(self.senderHostname, self.metricPrefix+".memstore.series-command.count", netmodel.Int64(self.memstore.SeriesCommandCount())).SetTimestamp(timestamp)
	seriesCommands = append(seriesCommands, seriesCommand)
	seriesCommand = netmodel.NewSeriesCommand(self.senderHostname, self.metricPrefix+".memstore.size", netmodel.Int64(self.memstore.Size())).SetTimestamp(timestamp)
	seriesCommands = append(seriesCommands, seriesCommand)
	self.writeCommunicator.PriorSendData(seriesCommands, nil, nil, nil)

}

func (self *Storage) SendSeriesCommands(seriesCommands []*netmodel.SeriesCommand) {
	self.memstore.AppendSeriesCommands(seriesCommands)
}
func (self *Storage) SendPropertyCommands(propertyCommands []*netmodel.PropertyCommand) {
	self.memstore.AppendPropertyCommands(propertyCommands)
}
func (self *Storage) SendEntityTagCommands(entityTagCommands []*netmodel.EntityTagCommand) {
	self.memstore.AppendEntityTagCommands(entityTagCommands)
}
func (self *Storage) SendMessageCommands(messageCommands []*netmodel.MessageCommand) {
	self.memstore.AppendMessageCommands(messageCommands)
}

func (self *Storage) RegisterMetric(metric *httpmodel.Metric) error {
	return self.atsdHttpClient.Metric.CreateOrReplace(metric)
}

func (self *Storage) StartPeriodicSending() {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if !self.isUpdating {
		schedule(self.updateTask, self.updateInterval)
	}
}
func (self *Storage) StopPeriodicSending() {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if self.isUpdating {
		self.stopUpdateTask <- true
	}
}
func (self *Storage) ForceSend() {
	self.updateTask()
}

func schedule(task func(), updateInterval time.Duration) chan bool {
	stop := make(chan bool)
	go func() {
		ticker := time.NewTicker(updateInterval)
		for {
			select {
			case <-ticker.C:
				task()
			case <-stop:
				ticker.Stop()
				return
			}
		}
	}()
	return stop
}
