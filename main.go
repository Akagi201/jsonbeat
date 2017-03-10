package main

import (
	"os"

	"github.com/elastic/beats/libbeat/beat"

	"github.com/Akagi201/jsonbeat/beater"
)

func main() {
	err := beat.Run("jsonbeat", "", beater.New)
	if err != nil {
		os.Exit(1)
	}
}
