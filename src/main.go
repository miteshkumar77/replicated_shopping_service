package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"time"
)

const timeout = time.Duration(250 * time.Millisecond)

type Node struct {
	TcpStartPort int    `json:"tcp_start_port"`
	TcpEndPort   int    `json:"tcp_end_port"`
	UdpStartPort int    `json:"udp_start_port"`
	UdpEndPort   int    `json:"udp_end_port"`
	IpAddress    string `json:"ip_address"`
}

type Map struct {
	Hosts map[string]Node `json:"hosts"`
}

func main() {

	args := os.Args

	if len(args) != 2 {
		log.Fatal("USAGE: ./main <site_id>")
	}
	// site_id := args[1]
	knownhosts_f, err := os.Open("knownhosts.json")

	if err != nil {
		log.Fatalf("Error opening knownhosts.json: %v\r\n", err)
	}

	byteArr, _ := ioutil.ReadAll(knownhosts_f)

	var ntwk Map
	err = json.Unmarshal(byteArr, &ntwk)
	if err != nil {
		log.Fatalf("Error unmarshalling in main: %v\n", err)
	}
}
