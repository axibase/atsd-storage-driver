package storage

import (
	"github.com/axibase/atsd-api-go/net/model"
	"reflect"
	"testing"
	"time"
)

func TestDataCompacter(t *testing.T) {
	cases := []*struct {
		Name        string
		GroupParams map[string]DeduplicationParams
		Group       map[string]struct {
			InputSeriesCommands    []*model.SeriesCommand
			ExpectedSeriesCommands []*model.SeriesCommand
		}
	}{
		{
			Name: "Testing interval behavior",
			GroupParams: map[string]DeduplicationParams{
				"test01": DeduplicationParams{Threshold: 0, Interval: 1 * time.Second},
				"test02": DeduplicationParams{Threshold: 0, Interval: 2 * time.Second},
				"test03": DeduplicationParams{Threshold: 0, Interval: 3 * time.Second},
				"test04": DeduplicationParams{Threshold: 0, Interval: 4 * time.Second},
				"test05": DeduplicationParams{Threshold: 0, Interval: 5 * time.Second},
			},
			Group: map[string]struct {
				InputSeriesCommands    []*model.SeriesCommand
				ExpectedSeriesCommands []*model.SeriesCommand
			}{
				"test01": {
					InputSeriesCommands: []*model.SeriesCommand{
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(1000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(2000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(3000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(4000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(5000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(6000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(7000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(8000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(9000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(10000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(11000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(12000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(13000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(14000)),
					},
					ExpectedSeriesCommands: []*model.SeriesCommand{
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(1000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(2000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(3000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(4000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(5000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(6000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(7000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(8000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(9000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(10000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(11000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(12000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(13000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(14000)),
					},
				},
				"test02": {
					InputSeriesCommands: []*model.SeriesCommand{
						model.NewSeriesCommand("entity002", "metric002", model.Float64(200)).SetTimestamp(model.Millis(1000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(200)).SetTimestamp(model.Millis(2000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(200)).SetTimestamp(model.Millis(3000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(200)).SetTimestamp(model.Millis(4000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(200)).SetTimestamp(model.Millis(5000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(200)).SetTimestamp(model.Millis(6000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(200)).SetTimestamp(model.Millis(7000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(200)).SetTimestamp(model.Millis(8000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(200)).SetTimestamp(model.Millis(9000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(200)).SetTimestamp(model.Millis(10000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(200)).SetTimestamp(model.Millis(11000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(200)).SetTimestamp(model.Millis(12000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(200)).SetTimestamp(model.Millis(13000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(200)).SetTimestamp(model.Millis(14000)),
					},
					ExpectedSeriesCommands: []*model.SeriesCommand{
						model.NewSeriesCommand("entity002", "metric002", model.Float64(200)).SetTimestamp(model.Millis(1000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(200)).SetTimestamp(model.Millis(3000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(200)).SetTimestamp(model.Millis(5000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(200)).SetTimestamp(model.Millis(7000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(200)).SetTimestamp(model.Millis(9000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(200)).SetTimestamp(model.Millis(11000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(200)).SetTimestamp(model.Millis(13000)),
					},
				},
				"test03": {
					InputSeriesCommands: []*model.SeriesCommand{
						model.NewSeriesCommand("entity003", "metric003", model.Float64(300)).SetTimestamp(model.Millis(1000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(300)).SetTimestamp(model.Millis(2000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(300)).SetTimestamp(model.Millis(3000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(300)).SetTimestamp(model.Millis(4000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(300)).SetTimestamp(model.Millis(5000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(300)).SetTimestamp(model.Millis(6000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(300)).SetTimestamp(model.Millis(7000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(300)).SetTimestamp(model.Millis(8000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(300)).SetTimestamp(model.Millis(9000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(300)).SetTimestamp(model.Millis(10000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(300)).SetTimestamp(model.Millis(11000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(300)).SetTimestamp(model.Millis(12000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(300)).SetTimestamp(model.Millis(13000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(300)).SetTimestamp(model.Millis(14000)),
					},
					ExpectedSeriesCommands: []*model.SeriesCommand{
						model.NewSeriesCommand("entity003", "metric003", model.Float64(300)).SetTimestamp(model.Millis(1000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(300)).SetTimestamp(model.Millis(4000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(300)).SetTimestamp(model.Millis(7000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(300)).SetTimestamp(model.Millis(10000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(300)).SetTimestamp(model.Millis(13000)),
					},
				},
				"test04": {
					InputSeriesCommands: []*model.SeriesCommand{
						model.NewSeriesCommand("entity004", "metric004", model.Float64(400)).SetTimestamp(model.Millis(1000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(400)).SetTimestamp(model.Millis(2000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(400)).SetTimestamp(model.Millis(3000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(400)).SetTimestamp(model.Millis(4000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(400)).SetTimestamp(model.Millis(5000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(400)).SetTimestamp(model.Millis(6000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(400)).SetTimestamp(model.Millis(7000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(400)).SetTimestamp(model.Millis(8000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(400)).SetTimestamp(model.Millis(9000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(400)).SetTimestamp(model.Millis(10000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(400)).SetTimestamp(model.Millis(11000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(400)).SetTimestamp(model.Millis(12000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(400)).SetTimestamp(model.Millis(13000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(400)).SetTimestamp(model.Millis(14000)),
					},
					ExpectedSeriesCommands: []*model.SeriesCommand{
						model.NewSeriesCommand("entity004", "metric004", model.Float64(400)).SetTimestamp(model.Millis(1000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(400)).SetTimestamp(model.Millis(5000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(400)).SetTimestamp(model.Millis(9000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(400)).SetTimestamp(model.Millis(13000)),
					},
				},
				"test05": {
					InputSeriesCommands: []*model.SeriesCommand{
						model.NewSeriesCommand("entity005", "metric005", model.Float64(500)).SetTimestamp(model.Millis(1000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(500)).SetTimestamp(model.Millis(2000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(500)).SetTimestamp(model.Millis(3000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(500)).SetTimestamp(model.Millis(4000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(500)).SetTimestamp(model.Millis(5000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(500)).SetTimestamp(model.Millis(6000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(500)).SetTimestamp(model.Millis(7000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(500)).SetTimestamp(model.Millis(8000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(500)).SetTimestamp(model.Millis(9000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(500)).SetTimestamp(model.Millis(10000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(500)).SetTimestamp(model.Millis(11000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(500)).SetTimestamp(model.Millis(12000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(500)).SetTimestamp(model.Millis(13000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(500)).SetTimestamp(model.Millis(14000)),
					},
					ExpectedSeriesCommands: []*model.SeriesCommand{
						model.NewSeriesCommand("entity005", "metric005", model.Float64(500)).SetTimestamp(model.Millis(1000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(500)).SetTimestamp(model.Millis(6000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(500)).SetTimestamp(model.Millis(11000)),
					},
				},
			},
		},
		{
			Name: "Testing threshold behavior",
			GroupParams: map[string]DeduplicationParams{
				"test01": DeduplicationParams{Threshold: 0.1, Interval: time.Minute},
				"test02": DeduplicationParams{Threshold: 0.2, Interval: time.Minute},
				"test03": DeduplicationParams{Threshold: 0.3, Interval: time.Minute},
				"test04": DeduplicationParams{Threshold: 0.4, Interval: time.Minute},
				"test05": DeduplicationParams{Threshold: 0.5, Interval: time.Minute},
				"test06": DeduplicationParams{Threshold: 0, Interval: time.Minute},
			},
			Group: map[string]struct {
				InputSeriesCommands    []*model.SeriesCommand
				ExpectedSeriesCommands []*model.SeriesCommand
			}{
				"test01": {
					InputSeriesCommands: []*model.SeriesCommand{
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(1000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(110)).SetTimestamp(model.Millis(2000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(120)).SetTimestamp(model.Millis(3000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(130)).SetTimestamp(model.Millis(4000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(140)).SetTimestamp(model.Millis(5000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(150)).SetTimestamp(model.Millis(6000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(160)).SetTimestamp(model.Millis(7000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(170)).SetTimestamp(model.Millis(8000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(180)).SetTimestamp(model.Millis(9000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(190)).SetTimestamp(model.Millis(10000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(200)).SetTimestamp(model.Millis(11000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(210)).SetTimestamp(model.Millis(12000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(220)).SetTimestamp(model.Millis(13000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(230)).SetTimestamp(model.Millis(14000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(240)).SetTimestamp(model.Millis(15000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(250)).SetTimestamp(model.Millis(16000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(260)).SetTimestamp(model.Millis(17000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(270)).SetTimestamp(model.Millis(18000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(280)).SetTimestamp(model.Millis(19000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(290)).SetTimestamp(model.Millis(20000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(300)).SetTimestamp(model.Millis(21000)),
					},
					ExpectedSeriesCommands: []*model.SeriesCommand{
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(1000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(120)).SetTimestamp(model.Millis(3000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(140)).SetTimestamp(model.Millis(5000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(160)).SetTimestamp(model.Millis(7000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(180)).SetTimestamp(model.Millis(9000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(200)).SetTimestamp(model.Millis(11000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(230)).SetTimestamp(model.Millis(14000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(260)).SetTimestamp(model.Millis(17000)),
						model.NewSeriesCommand("entity001", "metric001", model.Float64(290)).SetTimestamp(model.Millis(20000)),
					},
				},
				"test02": {
					InputSeriesCommands: []*model.SeriesCommand{
						model.NewSeriesCommand("entity002", "metric002", model.Float64(100)).SetTimestamp(model.Millis(1000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(110)).SetTimestamp(model.Millis(2000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(120)).SetTimestamp(model.Millis(3000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(130)).SetTimestamp(model.Millis(4000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(140)).SetTimestamp(model.Millis(5000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(150)).SetTimestamp(model.Millis(6000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(160)).SetTimestamp(model.Millis(7000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(170)).SetTimestamp(model.Millis(8000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(180)).SetTimestamp(model.Millis(9000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(190)).SetTimestamp(model.Millis(10000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(200)).SetTimestamp(model.Millis(11000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(210)).SetTimestamp(model.Millis(12000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(220)).SetTimestamp(model.Millis(13000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(230)).SetTimestamp(model.Millis(14000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(240)).SetTimestamp(model.Millis(15000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(250)).SetTimestamp(model.Millis(16000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(260)).SetTimestamp(model.Millis(17000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(270)).SetTimestamp(model.Millis(18000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(280)).SetTimestamp(model.Millis(19000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(290)).SetTimestamp(model.Millis(20000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(300)).SetTimestamp(model.Millis(21000)),
					},
					ExpectedSeriesCommands: []*model.SeriesCommand{
						model.NewSeriesCommand("entity002", "metric002", model.Float64(100)).SetTimestamp(model.Millis(1000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(130)).SetTimestamp(model.Millis(4000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(160)).SetTimestamp(model.Millis(7000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(200)).SetTimestamp(model.Millis(11000)),
						model.NewSeriesCommand("entity002", "metric002", model.Float64(250)).SetTimestamp(model.Millis(16000)),
					},
				},
				"test03": {
					InputSeriesCommands: []*model.SeriesCommand{
						model.NewSeriesCommand("entity003", "metric003", model.Float64(100)).SetTimestamp(model.Millis(1000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(110)).SetTimestamp(model.Millis(2000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(120)).SetTimestamp(model.Millis(3000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(130)).SetTimestamp(model.Millis(4000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(140)).SetTimestamp(model.Millis(5000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(150)).SetTimestamp(model.Millis(6000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(160)).SetTimestamp(model.Millis(7000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(170)).SetTimestamp(model.Millis(8000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(180)).SetTimestamp(model.Millis(9000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(190)).SetTimestamp(model.Millis(10000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(200)).SetTimestamp(model.Millis(11000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(210)).SetTimestamp(model.Millis(12000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(220)).SetTimestamp(model.Millis(13000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(230)).SetTimestamp(model.Millis(14000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(240)).SetTimestamp(model.Millis(15000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(250)).SetTimestamp(model.Millis(16000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(260)).SetTimestamp(model.Millis(17000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(270)).SetTimestamp(model.Millis(18000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(280)).SetTimestamp(model.Millis(19000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(290)).SetTimestamp(model.Millis(20000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(300)).SetTimestamp(model.Millis(21000)),
					},
					ExpectedSeriesCommands: []*model.SeriesCommand{
						model.NewSeriesCommand("entity003", "metric003", model.Float64(100)).SetTimestamp(model.Millis(1000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(140)).SetTimestamp(model.Millis(5000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(190)).SetTimestamp(model.Millis(10000)),
						model.NewSeriesCommand("entity003", "metric003", model.Float64(250)).SetTimestamp(model.Millis(16000)),
					},
				},
				"test04": {
					InputSeriesCommands: []*model.SeriesCommand{
						model.NewSeriesCommand("entity004", "metric004", model.Float64(100)).SetTimestamp(model.Millis(1000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(110)).SetTimestamp(model.Millis(2000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(120)).SetTimestamp(model.Millis(3000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(130)).SetTimestamp(model.Millis(4000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(140)).SetTimestamp(model.Millis(5000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(150)).SetTimestamp(model.Millis(6000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(160)).SetTimestamp(model.Millis(7000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(170)).SetTimestamp(model.Millis(8000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(180)).SetTimestamp(model.Millis(9000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(190)).SetTimestamp(model.Millis(10000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(200)).SetTimestamp(model.Millis(11000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(210)).SetTimestamp(model.Millis(12000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(220)).SetTimestamp(model.Millis(13000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(230)).SetTimestamp(model.Millis(14000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(240)).SetTimestamp(model.Millis(15000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(250)).SetTimestamp(model.Millis(16000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(260)).SetTimestamp(model.Millis(17000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(270)).SetTimestamp(model.Millis(18000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(280)).SetTimestamp(model.Millis(19000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(290)).SetTimestamp(model.Millis(20000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(300)).SetTimestamp(model.Millis(21000)),
					},
					ExpectedSeriesCommands: []*model.SeriesCommand{
						model.NewSeriesCommand("entity004", "metric004", model.Float64(100)).SetTimestamp(model.Millis(1000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(150)).SetTimestamp(model.Millis(6000)),
						model.NewSeriesCommand("entity004", "metric004", model.Float64(220)).SetTimestamp(model.Millis(13000)),
					},
				},
				"test05": {
					InputSeriesCommands: []*model.SeriesCommand{
						model.NewSeriesCommand("entity005", "metric005", model.Float64(100)).SetTimestamp(model.Millis(1000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(110)).SetTimestamp(model.Millis(2000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(120)).SetTimestamp(model.Millis(3000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(130)).SetTimestamp(model.Millis(4000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(140)).SetTimestamp(model.Millis(5000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(150)).SetTimestamp(model.Millis(6000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(160)).SetTimestamp(model.Millis(7000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(170)).SetTimestamp(model.Millis(8000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(180)).SetTimestamp(model.Millis(9000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(190)).SetTimestamp(model.Millis(10000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(200)).SetTimestamp(model.Millis(11000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(210)).SetTimestamp(model.Millis(12000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(220)).SetTimestamp(model.Millis(13000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(230)).SetTimestamp(model.Millis(14000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(240)).SetTimestamp(model.Millis(15000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(250)).SetTimestamp(model.Millis(16000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(260)).SetTimestamp(model.Millis(17000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(270)).SetTimestamp(model.Millis(18000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(280)).SetTimestamp(model.Millis(19000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(290)).SetTimestamp(model.Millis(20000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(300)).SetTimestamp(model.Millis(21000)),
					},
					ExpectedSeriesCommands: []*model.SeriesCommand{
						model.NewSeriesCommand("entity005", "metric005", model.Float64(100)).SetTimestamp(model.Millis(1000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(160)).SetTimestamp(model.Millis(7000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(250)).SetTimestamp(model.Millis(16000)),
					},
				},
				"test06": {
					InputSeriesCommands: []*model.SeriesCommand{
						model.NewSeriesCommand("entity005", "metric005", model.Float64(100)).SetTimestamp(model.Millis(1000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(100)).SetTimestamp(model.Millis(2000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(100)).SetTimestamp(model.Millis(3000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(100)).SetTimestamp(model.Millis(4000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(100)).SetTimestamp(model.Millis(5000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(100)).SetTimestamp(model.Millis(6000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(160)).SetTimestamp(model.Millis(7000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(170)).SetTimestamp(model.Millis(8000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(180)).SetTimestamp(model.Millis(9000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(190)).SetTimestamp(model.Millis(10000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(200)).SetTimestamp(model.Millis(11000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(210)).SetTimestamp(model.Millis(12000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(220)).SetTimestamp(model.Millis(13000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(230)).SetTimestamp(model.Millis(14000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(240)).SetTimestamp(model.Millis(15000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(250)).SetTimestamp(model.Millis(16000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(260)).SetTimestamp(model.Millis(17000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(270)).SetTimestamp(model.Millis(18000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(280)).SetTimestamp(model.Millis(19000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(290)).SetTimestamp(model.Millis(20000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(300)).SetTimestamp(model.Millis(21000)),
					},
					ExpectedSeriesCommands: []*model.SeriesCommand{
						model.NewSeriesCommand("entity005", "metric005", model.Float64(100)).SetTimestamp(model.Millis(1000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(160)).SetTimestamp(model.Millis(7000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(170)).SetTimestamp(model.Millis(8000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(180)).SetTimestamp(model.Millis(9000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(190)).SetTimestamp(model.Millis(10000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(200)).SetTimestamp(model.Millis(11000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(210)).SetTimestamp(model.Millis(12000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(220)).SetTimestamp(model.Millis(13000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(230)).SetTimestamp(model.Millis(14000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(240)).SetTimestamp(model.Millis(15000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(250)).SetTimestamp(model.Millis(16000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(260)).SetTimestamp(model.Millis(17000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(270)).SetTimestamp(model.Millis(18000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(280)).SetTimestamp(model.Millis(19000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(290)).SetTimestamp(model.Millis(20000)),
						model.NewSeriesCommand("entity005", "metric005", model.Float64(300)).SetTimestamp(model.Millis(21000)),
					},
				},
			},
		},
		{
			Name: "Testing group behavior",
			GroupParams: map[string]DeduplicationParams{
				"test02": DeduplicationParams{Threshold: 0.5, Interval: time.Minute},
			},
			Group: map[string]struct {
				InputSeriesCommands    []*model.SeriesCommand
				ExpectedSeriesCommands []*model.SeriesCommand
			}{
				"test01": {
					InputSeriesCommands: []*model.SeriesCommand{
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(1000)),
					},
					ExpectedSeriesCommands: []*model.SeriesCommand{
						model.NewSeriesCommand("entity001", "metric001", model.Float64(100)).SetTimestamp(model.Millis(1000)),
					},
				},
			},
		},
	}

	for _, c := range cases {
		dataCompacter := DataCompacter{Buffer: map[string]map[string]sample{}, GroupParams: c.GroupParams}

		for groupName, io := range c.Group {
			filteredSeries := dataCompacter.Filter(groupName, io.InputSeriesCommands)

			if !reflect.DeepEqual(filteredSeries, io.ExpectedSeriesCommands) {
				t.Error(c.Name, groupName, " unexpected result: ", filteredSeries, io.ExpectedSeriesCommands)
			}
		}
	}
}
