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
	"github.com/axibase/atsd-api-go/net/model"
	"sync"
	"time"
)

type Storage struct {
	memstore          *MemStore
	writeCommunicator WriteCommunicator

	updateTask     func()
	isUpdating     bool
	updateInterval time.Duration
	stopUpdateTask chan bool
	mutex          sync.Mutex
}

func New(memstoreLimit uint64, writeCommunicator WriteCommunicator, updateInterval time.Duration) *Storage {
	memstore := NewMemStore(memstoreLimit)
	updateTask := func() {
		seriesCommandsChunkMap := memstore.ReleaseSeriesCommandMap()
		properties := memstore.ReleaseProperties()
		entityTagCommands := memstore.ReleaseEntityTagCommands()

		seriesCommandsChunks := []*list.List{}
		for _, val := range *seriesCommandsChunkMap {
			seriesCommandsChunks = append(seriesCommandsChunks, val)
		}
		writeCommunicator.SendData(seriesCommandsChunks, entityTagCommands, properties)
	}

	return &Storage{
		memstore:          memstore,
		writeCommunicator: writeCommunicator,
		updateTask:        updateTask,
		updateInterval:    updateInterval,
		isUpdating:        false,
	}
}

func (self *Storage) SendSeriesCommands(seriesCommands []*model.SeriesCommand) {
	self.memstore.AppendSeriesCommands(seriesCommands)
}
func (self *Storage) SendPropertyCommands(propertyCommands []*model.PropertyCommand) {
	self.memstore.AppendPropertyCommands(propertyCommands)
}
func (self *Storage) SendEntityTagCommands(entityTagCommands []*model.EntityTagCommand) {
	self.memstore.AppendEntityTagCommands(entityTagCommands)
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
