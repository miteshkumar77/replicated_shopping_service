package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

const timeout = time.Duration(250 * time.Millisecond)
const max_pkt_size = 1024

type Node struct {
	TcpStartPort int    `json:"tcp_start_port"`
	TcpEndPort   int    `json:"tcp_end_port"`
	UdpStartPort int    `json:"udp_start_port"`
	UdpEndPort   int    `json:"udp_end_port"`
	IpAddress    string `json:"ip_address"`
}

/*
	site_id ->
		{
			tcp_start_port,
			tcp_end_port,
			udp_start_port,
			udp_end_port,
			ip_address
		}
*/

/*
Message types:
		-
*/
type Map struct {
	Hosts map[string]Node `json:"hosts"`
}

type Server struct {
	site_id string
	peers   map[string]Node
	stdin_c chan string
}

func newServer(site_id string, peers Map) Server {
	return Server{site_id: site_id, peers: peers.Hosts, stdin_c: make(chan string)}
}

func stdin_read_loop(stdin_c chan string, reader *bufio.Reader) {
	b := make([]byte, max_pkt_size)
	for {
		n, err := reader.Read(b)
		if err != nil {
			if err != io.EOF {
				log.Fatalf("Read error: %v\n", err)
			}
			break
		}
		stdin_c <- string(b[:n])
	}
}

func parse_int_list(line *string) []int {
	str_list := strings.Split(*line, ",")
	arr := make([]int, len(str_list))

	for i := 0; i < len(arr); i++ {
		tmp, err := strconv.ParseInt(str_list[i], 10, 31)
		if err != nil {
			return make([]int, 0)
		}
		arr[i] = int(tmp)
	}
	return arr
}

func (srv *Server) handle_order(name string, amounts []int) {
	fmt.Printf("order %s %v\n", name, amounts)
}

func (srv *Server) handle_cancel(name string) {
	fmt.Printf("cancel %s\n")
}

func (srv *Server) handle_list_orders() {
	fmt.Println("orders")
}

func (srv *Server) handle_list_inventory() {
	fmt.Println("inventory")
}

func (srv *Server) handle_send_to_site_id(site_id string) {
	fmt.Printf("send %s\n", site_id)
}

func (srv *Server) handle_sendall() {
	fmt.Println("sendall")
}

func (srv *Server) handle_list_log() {
	fmt.Println("log")
}

func (srv *Server) handle_list_clock() {
	fmt.Println("clock")
}

func (srv *Server) dump_to_stable_storage() {
	fmt.Println("quit")
}

func (srv *Server) run() {
	stdin_reader := bufio.NewReader(os.Stdin)
	go stdin_read_loop(srv.stdin_c, stdin_reader)

	for {
		select {
		case user_input := <-srv.stdin_c:
			args := strings.Fields(user_input)
			if len(args) == 0 {
				fmt.Println("invalid command")
			} else if args[0] == "order" {
				valid := true
				if len(args) != 3 {
					valid = false
				} else {
					name := args[1]
					amounts := parse_int_list(&args[2])
					if len(amounts) != 4 {
						valid = false
					} else {
						srv.handle_order(name, amounts)
					}
				}
				if !valid {
					fmt.Println("usage: order <customer_name> <#_of_masks>,<#_of_bottles>,<#_of_rolls>,<#_of_pbcups>")
				}
			} else if args[0] == "cancel" {
				if len(args) != 2 {
					fmt.Println("usage: cancel <customer_name>")
				} else {
					name := args[1]
					srv.handle_cancel(name)
				}

			} else if args[0] == "orders" {
				if len(args) != 1 {
					fmt.Println("usage: orders")
				} else {
					srv.handle_list_orders()
				}
			} else if args[0] == "inventory" {
				if len(args) != 1 {
					fmt.Println("usage: inventory")
				} else {
					srv.handle_list_inventory()
				}
			} else if args[0] == "send" {
				valid := true
				if len(args) != 2 {
					valid = false
				} else {
					site_id := args[1]
					_, exists := srv.peers[site_id]
					if !exists {
						valid = false
					} else {
						srv.handle_send_to_site_id(site_id)
					}
				}
				if !valid {
					fmt.Println("usage: send <site_id>")
				}
			} else if args[0] == "sendall" {
				if len(args) != 1 {
					fmt.Println("usage: sendall")
				} else {
					srv.handle_sendall()
				}
			} else if args[0] == "log" {
				if len(args) != 1 {
					fmt.Println("usage: log")
				} else {
					srv.handle_list_log()
				}
			} else if args[0] == "clock" {
				if len(args) != 1 {
					fmt.Println("usage: clock")
				} else {
					srv.handle_list_clock()
				}
			} else if args[0] == "quit" {
				srv.dump_to_stable_storage()
				return // EXIT POINT
			} else {
				fmt.Println("invalid command")
			}
		}
	}
}

func main() {

	args := os.Args

	if len(args) != 2 {
		log.Fatal("USAGE: ./main <site_id>")
	}
	site_id := args[1]
	knownhosts_f, err := os.Open("knownhosts.json")

	if err != nil {
		log.Fatalf("Error opening knownhosts.json: %v\r\n", err)
	}

	byteArr, _ := ioutil.ReadAll(knownhosts_f)

	var peers Map
	err = json.Unmarshal(byteArr, &peers)
	if err != nil {
		log.Fatalf("Error unmarshalling in main: %v\n", err)
	}
	s := newServer(site_id, peers)
	s.run()
}
