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
	"github.com/google/cadvisor/storage/atsd/net/model"
	"sort"
	"sync"
)

type MemStore struct {
	seriesCommandMap *map[string]*list.List

	properties []*model.PropertyCommand

	entityTagCommands []*model.EntityTagCommand

	sync.Mutex

	Limit uint64
}

func NewMemStore(limit uint64) *MemStore {
	ms := &MemStore{
		seriesCommandMap: &map[string]*list.List{},
		Limit:            limit,
	}
	return ms
}
func (self *MemStore) AppendSeriesCommands(commands []*model.SeriesCommand) {
	self.Lock()
	defer self.Unlock()
	if uint64(self.Size()) < self.Limit {
		for i := 0; i < len(commands); i++ {
			key := self.getKey(commands[i])
			if _, ok := (*self.seriesCommandMap)[key]; !ok {
				(*self.seriesCommandMap)[key] = list.New()
			}
			(*self.seriesCommandMap)[key].PushBack(commands[i])
		}
	}
}
func (self *MemStore) AppendPropertyCommands(propertyCommands []*model.PropertyCommand) {
	self.Lock()
	defer self.Unlock()
	if uint64(self.Size()) < self.Limit {
		self.properties = append(self.properties, propertyCommands...)
	}
}
func (self *MemStore) AppendEntityTagCommands(entityUpdateCommands []*model.EntityTagCommand) {
	self.Lock()
	defer self.Unlock()
	if uint64(self.Size()) < self.Limit {
		self.entityTagCommands = append(self.entityTagCommands, entityUpdateCommands...)
	}
}

func (self *MemStore) ReleaseSeriesCommandMap() *map[string]*list.List {
	self.Lock()
	defer self.Unlock()
	smap := self.seriesCommandMap
	self.seriesCommandMap = &map[string]*list.List{}
	return smap
}
func (self *MemStore) ReleaseProperties() []*model.PropertyCommand {
	self.Lock()
	defer self.Unlock()
	properties := self.properties
	self.properties = nil
	return properties
}
func (self *MemStore) ReleaseEntityTagCommands() []*model.EntityTagCommand {
	self.Lock()
	defer self.Unlock()
	entityTagCommands := self.entityTagCommands
	self.entityTagCommands = nil
	return entityTagCommands
}
func (self *MemStore) SeriesCommandCount() int {
	commandCount := 0

	for _, val := range *(self.seriesCommandMap) {
		commandCount += val.Len()
	}
	return commandCount
}
func (self *MemStore) PropertiesCount() int {
	return len(self.properties)
}
func (self *MemStore) EntitiesCount() int {
	return len(self.entityTagCommands)
}
func (self *MemStore) Size() uint64 {
	return uint64(self.EntitiesCount() + self.PropertiesCount() + self.SeriesCommandCount())
}

func (self *MemStore) getKey(sc *model.SeriesCommand) string {
	key := sc.Entity()
	metrics := []string{}
	for metricName := range sc.Metrics() {
		metrics = append(metrics, metricName)
	}
	sort.Strings(metrics)
	for i := range metrics {
		key += metrics[i]
	}

	tags := []string{}
	for tagName, tagValue := range sc.Tags() {
		tags = append(tags, tagName+"="+tagValue)
	}
	sort.Strings(tags)
	for i := range tags {
		key += tags[i]
	}

	return key
}
