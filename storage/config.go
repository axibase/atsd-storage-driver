package storage

import (
	"errors"
	neturl "net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Url              *neturl.URL
	MetricPrefix     string
	SelfMetricEntity string

	ConnectionLimit uint
	MemstoreLimit   uint

	InsecureSkipVerify bool

	UpdateInterval time.Duration

	GroupParams map[string]DeduplicationParams
}

func GetDefaultConfig() Config {
	urlStruct := &neturl.URL{
		Scheme: "tcp",
		User:   neturl.UserPassword("admin", "admin"),
		Host:   "localhost:8081",
	}
	hostname, _ := os.Hostname()
	return Config{
		Url:              urlStruct,
		MetricPrefix:     "storagedriver",
		SelfMetricEntity: hostname,
		ConnectionLimit:  1,
		MemstoreLimit:    1000000,
		UpdateInterval:   1 * time.Minute,
		GroupParams:      map[string]DeduplicationParams{},
	}
}

func (self *Config) UnmarshalTOML(data interface{}) error {
	d, _ := data.(map[string]interface{})

	if u, ok := d["url"]; ok {
		urlString, _ := u.(string)
		url, err := neturl.ParseRequestURI(urlString)
		if err != nil {
			return err
		}
		self.Url = url
	}

	if isv, ok := d["skip_verify"]; ok {
		InsecureSkipVerify, _ := isv.(bool)
		self.InsecureSkipVerify = InsecureSkipVerify
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
		self.MemstoreLimit = uint(memstoreLimit)
	}

	if ui, ok := d["update_interval"]; ok {
		updateInterval, _ := ui.(string)
		duration, err := time.ParseDuration(updateInterval)
		if err != nil {
			return errors.New("unknown update_interval format")
		}
		self.UpdateInterval = duration
	}

	if self.GroupParams == nil {
		self.GroupParams = map[string]DeduplicationParams{}
	}
	if g, ok := d["deduplication"]; ok {
		groups, _ := g.(map[string]interface{})
		for key, val := range groups {
			m := val.(map[string]interface{})
			thresholdString, _ := m["threshold"].(string)
			var threshold interface{}
			if strings.HasSuffix(thresholdString, "%") {
				val, err := strconv.ParseFloat(strings.TrimSuffix(thresholdString, "%"), 64)
				if err != nil {
					panic(err)
				}
				threshold = Percent(val)
			} else {
				val, err := strconv.ParseFloat(thresholdString, 64)
				if err != nil {
					panic(err)
				}
				threshold = Absolute(val)
			}

			intervalString, _ := m["interval"].(string)
			interval, err := time.ParseDuration(intervalString)
			if err != nil {
				return err
			}
			self.GroupParams[key] = DeduplicationParams{Threshold: threshold, Interval: interval}
		}
	}

	return nil
}
