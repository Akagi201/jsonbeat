package beater

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"

	"github.com/Akagi201/jsonbeat/config"
	"github.com/hpcloud/tail"
)

// Jsonbeat implements the Beater interface
type Jsonbeat struct {
	done   chan struct{}    // Channel used by the Run() method to stop when the Stop() method is called
	config config.Config    // Configuration options for the Beat
	client publisher.Client // Publisher that takes care of sending the events to the defined output
}

// New Creates the Beat object
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	config := config.DefaultConfig
	if err := cfg.Unpack(&config); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	bt := &Jsonbeat{
		done:   make(chan struct{}),
		config: config,
	}
	return bt, nil
}

// Run implements the Beater Run interface
// Contains the main application loop that captures data and sends it to the defined output using the publisher
func (bt *Jsonbeat) Run(b *beat.Beat) error {
	logp.Info("jsonbeat is running! Hit CTRL-C to stop it.")

	bt.client = b.Publisher.Connect()

	tailFileDone := make(chan struct{})
	tailFileconfig := tail.Config{
		ReOpen:      true,
		MustExist:   false,
		Poll:        false,
		Follow:      true,
		MaxLineSize: 0,
	}

	go bt.tailFile(bt.config.Path, tailFileconfig, tailFileDone, bt.done)

	<-tailFileDone

	return nil
}

// Stop implements the Beater Stop interface
// Contains logic that is called when the Beat is signaled to stop
func (bt *Jsonbeat) Stop() {
	bt.client.Close()
	close(bt.done)
}

func (bt *Jsonbeat) tailFile(filename string, config tail.Config, done chan struct{}, stop chan struct{}) {
	defer func() {
		done <- struct{}{}
	}()

	t, err := tail.TailFile(filename, config)
	if err != nil {
		logp.Err("Start tail file failed, err: %v", err)
		return
	}

	for line := range t.Lines {
		select {
		case <-stop:
			t.Stop()
			return
		default:
		}
		event := make(common.MapStr)
		if err = json.Unmarshal([]byte(line.Text), &event); err != nil {
			logp.Err("Unmarshal json log failed, err: %v", err)
			continue
		}
		if logTime, err := time.Parse("2017-03-13T07:13:30.172Z", event["@timestamp"].(string)); err != nil {
			event["@timestamp"] = common.Time(logTime)
		} else {
			logp.Err("Unmarshal json log @timestamp failed, time string: %v", event["@timestamp"].(string))
			event["@timestamp"] = common.Time(time.Now())
		}
		bt.client.PublishEvent(event)
		logp.Info("Event sent")
	}

	if err = t.Wait(); err != nil {
		logp.Err("Tail file blocking goroutine stopped, err: %v", err)
	}
}
