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
	"net/url"
	"time"
)

type StorageFactory interface {
	Create() *Storage
}

type NetworkStorageFactory struct {
	senderHostname   string
	metricPrefix     string
	limit            uint64
	protocol         string
	receiverHostport string
	connectionLimit  uint
	updateInterval   time.Duration
	url              *url.URL
	username         string
	password         string
}

func NewNetworkStorageFactory(senderHostname, protocol, receiverHostport string, url url.URL, username, password string, limit uint64, connectionLimit uint, updateInterval time.Duration, metricPrefix string) *NetworkStorageFactory {
	return &NetworkStorageFactory{
		senderHostname:   senderHostname,
		limit:            limit,
		protocol:         protocol,
		receiverHostport: receiverHostport,
		connectionLimit:  connectionLimit,
		updateInterval:   updateInterval,
		metricPrefix:     metricPrefix,
		url:              &url,
		username:         username,
		password:         password,
	}
}

func (self *NetworkStorageFactory) Create() *Storage {
	memstore := NewMemStore(self.limit)
	writeCommunicator := NewNetworkCommunicator(self.connectionLimit, self.protocol, self.receiverHostport)
	storage := &Storage{
		senderHostname:    self.senderHostname,
		memstore:          memstore,
		writeCommunicator: writeCommunicator,
		updateInterval:    self.updateInterval,
		isUpdating:        false,
		metricPrefix:      self.metricPrefix,
		atsdHttpClient:    http.New(*self.url, self.username, self.password),
	}
	schedule(storage.selfMetricSendTask, 15*time.Second)

	return storage
}

func NewHttpStorageFactory(senderHostname string, url url.URL, username, password string, limit uint64, updateInterval time.Duration, metricPrefix string) *HttpStorageFactory {
	return &HttpStorageFactory{
		senderHostname: senderHostname,
		limit:          limit,
		url:            &url,
		username:       username,
		password:       password,
		updateInterval: updateInterval,
		metricPrefix:   metricPrefix,
	}
}

type HttpStorageFactory struct {
	senderHostname string
	limit          uint64

	url      *url.URL
	username string
	password string

	updateInterval time.Duration
	metricPrefix   string
}

func (self *HttpStorageFactory) Create() *Storage {
	memstore := NewMemStore(self.limit)
	client := http.New(*self.url, self.username, self.password)
	writeCommunicator := NewHttpCommunicator(client)
	storage := &Storage{
		senderHostname:    self.senderHostname,
		memstore:          memstore,
		writeCommunicator: writeCommunicator,
		updateInterval:    self.updateInterval,
		isUpdating:        false,
		atsdHttpClient:    client,
		metricPrefix:      self.metricPrefix,
	}
	schedule(storage.selfMetricSendTask, 15*time.Second)
	return storage
}
