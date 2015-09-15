package storage

import (
	"errors"
	neturl "net/url"
	"time"
)

type Config struct {
	Url              *neturl.URL
	ReceiverHostport string
	Protocol         string
	MetricPrefix     string
	SelfMetricEntity string

	ConnectionLimit uint
	MemstoreLimit   uint64

	Username string
	Password string

	UpdateInterval time.Duration
}

func GetDefaultConfig() Config {
	urlStruct, _ := neturl.ParseRequestURI("http://localhost:8088")
	return Config{
		Url:              urlStruct,
		ReceiverHostport: "localhost:8082",
		Protocol:         "tcp",
		MetricPrefix:     "storagedriver",
		SelfMetricEntity: "hostname",
		ConnectionLimit:  uint(1),
		MemstoreLimit:    uint64(1000000),
		Username:         "admin",
		Password:         "admin",
		UpdateInterval:   1 * time.Minute,
	}
}

func (self *Config) UnmarshalTOML(data interface{}) error {
	d, _ := data.(map[string]interface{})

	defaultConf := GetDefaultConfig()
	self.Url = defaultConf.Url
	self.ReceiverHostport = defaultConf.ReceiverHostport
	self.ReceiverHostport = defaultConf.ReceiverHostport
	self.Protocol = defaultConf.Protocol
	self.MetricPrefix = defaultConf.MetricPrefix
	self.SelfMetricEntity = defaultConf.SelfMetricEntity
	self.ConnectionLimit = defaultConf.ConnectionLimit
	self.MemstoreLimit = defaultConf.MemstoreLimit
	self.Username = defaultConf.Username
	self.Password = defaultConf.Password
	self.UpdateInterval = defaultConf.UpdateInterval

	if u, ok := d["url"]; ok {
		urlString, _ := u.(string)
		url, err := neturl.ParseRequestURI(urlString)
		if err != nil {
			return err
		}
		self.Url = url
	}

	if wh, ok := d["write_host"]; ok {
		writeHost, _ := wh.(string)
		self.ReceiverHostport = writeHost
	}

	if p, ok := d["write_protocol"]; ok {
		protocol, _ := p.(string)
		switch protocol {
		case "tcp", "udp", "http/https":
			self.Protocol = protocol
		default:
			return errors.New("Unknown protocol type")
		}

	}

	if mp, ok := d["metric_prefix"]; ok {
		metricPrefix, _ := mp.(string)
		self.MetricPrefix = metricPrefix
	}

	if sme, ok := d["self_metric_entity"]; ok {
		selfMetricEntity, _ := sme.(string)
		self.SelfMetricEntity = selfMetricEntity
	}

	if cl, ok := d["connection_limit"]; ok {
		connectionLimit, _ := cl.(int64)
		self.ConnectionLimit = uint(connectionLimit)
	}

	if ml, ok := d["memstore_limit"]; ok {
		memstoreLimit, _ := ml.(int64)
		self.MemstoreLimit = uint64(memstoreLimit)
	}

	if u, ok := d["username"]; ok {
		username, _ := u.(string)
		self.Username = username
	}
	if p, ok := d["password"]; ok {
		password, _ := p.(string)
		self.Password = password
	}

	if ui, ok := d["update_interval"]; ok {
		updateInterval, _ := ui.(string)
		duration, err := time.ParseDuration(updateInterval)
		if err != nil {
			return errors.New("unknown update_interval format")
		}
		self.UpdateInterval = duration

	}

	return nil
}
