package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const timeout = time.Duration(250 * time.Millisecond)
const max_pkt_size = 1024
const max_dump_size = 1000000

type StatusCode int

const (
	filled StatusCode = iota
	pending
)

type LogEntryType int

const (
	logOrder LogEntryType = iota
	logCancel
)

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

type Order struct {
	Amounts [4]int     `json:"amounts"`
	Status  StatusCode `json:"status"`
}

type LogEntry struct {
	Type    LogEntryType `json:"type"`
	Name    string       `json:"name"`
	Amounts [4]int       `json:"amounts"`
	Time    int          `json:"time"`
	Site    string       `json:"site"`
}

type LogRecord struct {
	Dictionary map[string]Order          `json:"dictionary"`
	PartialLog []LogEntry                `json:"partial_log"`
	TimeVector map[string]map[string]int `json:"time_vector"`
	Amounts    [4]int                    `json:"amounts"`
}

func newLogRecord(peers map[string]Node) *LogRecord {
	TimeVector := make(map[string]map[string]int)
	for site_i := range peers {
		for site_j := range peers {
			TimeVector[site_i][site_j] = 0
		}
	}

	return &LogRecord{
		Dictionary: make(map[string]Order),
		PartialLog: make([]LogEntry, 0),
		TimeVector: TimeVector,
		Amounts:    [4]int{500, 100, 200, 200}}
}

type Message struct {
	NP     []LogEntry `json:"np"`
	T      [][]int    `json:"t"`
	Sender string     `json:"sender"`
}

type Server struct {
	site_id    string
	peers      map[string]Node
	peer_addrs map[string]*net.UDPAddr
	stdin_c    chan string
	netwk_c    chan Message
	record     LogRecord
}

func newServer(site_id string, peers Map) *Server {
	var record LogRecord
	record_file, err := os.Open("stable_storage.json")
	if err == nil {
		byteArr, err_read_json := ioutil.ReadAll(record_file)
		if err_read_json != nil {
			log.Fatalf("Error reading stable_storage.json: %v\n", err_read_json)
		}
		err_unmarshal := json.Unmarshal(byteArr, &record)
		if err_unmarshal != nil {
			log.Fatalf("stable_storage unmarshal error: %v\n", err_unmarshal)
		}
		fmt.Printf("record_file.Close(): %v\n", record_file.Close())
	} else {
		record = *newLogRecord(peers.Hosts)
	}
	s := Server{site_id: site_id,
		peers:      peers.Hosts,
		peer_addrs: make(map[string]*net.UDPAddr),
		stdin_c:    make(chan string),
		netwk_c:    make(chan Message),
		record:     record}

	for id, node := range s.peers {
		s.peer_addrs[id] = &net.UDPAddr{
			Port: node.UdpStartPort,
			IP:   net.ParseIP(node.IpAddress)}
	}
	return &s
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

func netwk_read_loop(netwk_c chan Message, reader *net.UDPConn) {
	b := make([]byte, max_pkt_size)
	for {
		n, err := reader.Read(b)
		if err != nil {
			if err != io.EOF {
				log.Fatalf("Read error: %v\n", err)
			}
			break
		}
		var m Message
		json.Unmarshal(b[:n], &m)
		netwk_c <- m
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

func (srv *Server) clock_tick() {
	srv.record.TimeVector[srv.site_id][srv.site_id]++
}

func (srv *Server) curr_time() int {
	return srv.record.TimeVector[srv.site_id][srv.site_id]
}

func (srv *Server) handle_order(name string, amounts [4]int) {
	for i := 0; i < 4; i++ {
		if amounts[i] > srv.record.Amounts[i] {
			fmt.Fprintf(os.Stdout, "Cannot place order for %s.\n", name)
			return
		}
	}

	srv.clock_tick()

	srv.record.Dictionary[name] = Order{
		Amounts: amounts,
		Status:  pending}

	srv.record.PartialLog = append(srv.record.PartialLog,
		LogEntry{
			Type:    logOrder,
			Name:    name,
			Amounts: amounts,
			Time:    srv.curr_time(),
			Site:    srv.site_id})

	srv.dump_to_stable_storage()

	fmt.Fprintf(os.Stdout, "Order submitted for %s.\n", name)
}

func (srv *Server) handle_cancel(name string) {

	if srv.record.Dictionary[name].Status == filled {
		fmt.Fprintf(os.Stdout, "Cannot cancel order for %s.\n", name)
		return
	}
	srv.clock_tick()
	srv.record.PartialLog = append(srv.record.PartialLog,
		LogEntry{
			Type:    logCancel,
			Name:    name,
			Amounts: [4]int{-1, -1, -1, -1},
			Time:    srv.curr_time(),
			Site:    srv.site_id})
	delete(srv.record.Dictionary, name)
	srv.dump_to_stable_storage()
	fmt.Fprintf(os.Stdout, "Reservation for %s cancelled.\n", name)
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
	record_file, err := os.OpenFile("stable_storage.json",
		os.O_TRUNC|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		log.Fatalf("stable_storage.json open error: %v\n", err)
	}

	b := make([]byte, max_dump_size)
	json.Unmarshal(b, &srv.record)
	_, err = record_file.Write(b)
	if err != nil {
		log.Fatalf("stable_storage.json write error: %v\n", err)
	}
	fmt.Printf("record_file.Close(): %v\n", record_file.Close())
}

func (srv *Server) handle_receive(mesg *Message) {
	fmt.Println("receive")
}

func (srv *Server) on_user_input(user_input string) {
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
				var amt [4]int
				copy(amt[:], amounts[0:4])
				srv.handle_order(name, amt)
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

func (srv *Server) run() {
	stdin_reader := bufio.NewReader(os.Stdin)
	ser, err := net.ListenUDP("udp", srv.peer_addrs[srv.site_id])
	if err != nil {
		log.Fatalf("ListenUDP error: %v\n", err)
	}

	go stdin_read_loop(srv.stdin_c, stdin_reader)
	go netwk_read_loop(srv.netwk_c, ser)

	for {
		select {
		case user_input := <-srv.stdin_c:
			srv.on_user_input(user_input)
			break
		case mesg := <-srv.netwk_c:
			srv.handle_receive(&mesg)
			break
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
	fmt.Printf("knownhosts_f.Close(): %v\n", knownhosts_f.Close())
	var peers Map
	err = json.Unmarshal(byteArr, &peers)
	if err != nil {
		log.Fatalf("Error unmarshalling in main: %v\n", err)
	}
	s := newServer(site_id, peers)
	s.run()
}
