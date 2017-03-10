package beater

import (
	"fmt"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"

	"github.com/Akagi201/jsonbeat/config"
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
	ticker := time.NewTicker(bt.config.Period)
	counter := 1
	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}

		event := common.MapStr{
			"@timestamp": common.Time(time.Now()),
			"type":       b.Name,
			"counter":    counter,
		}
		bt.client.PublishEvent(event)
		logp.Info("Event sent")
		counter++
	}
}

// Stop implements the Beater Stop interface
// Contains logic that is called when the Beat is signaled to stop
func (bt *Jsonbeat) Stop() {
	bt.client.Close()
	close(bt.done)
}
