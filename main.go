package main

import (
	"Client-WebDAV-go/cmd"
	"log"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
