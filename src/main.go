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
	"sort"
	"strconv"
	"strings"
	"time"
)

const timeout = time.Duration(250 * time.Millisecond)
const max_pkt_size = 1024
const max_dump_size = 1000000

var item_names = [4]string{
	"surgical masks",
	"hand sanitizer bottles",
	"toilet paper rolls",
	"reese's peanut butter cups"}

type StatusCode int

const (
	filled StatusCode = iota
	pending
)

func strStatus(sc StatusCode) string {
	switch sc {
	case filled:
		return "filled"
	case pending:
		return "pending"
	default:
		return ""
	}
}

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
	NP     []LogEntry                `json:"np"`
	T      map[string]map[string]int `json:"t"`
	Sender string                    `json:"sender"`
}

type Server struct {
	site_id    string
	peers      map[string]Node
	peer_addrs map[string]*net.UDPAddr
	stdin_c    chan string
	netwk_c    chan Message
	record     LogRecord
}

func amountsStr(amounts [4]int) string {
	var amountsStrVec [4]string
	for i := 0; i < 4; i++ {
		amountsStrVec[i] = fmt.Sprint(amounts[i])
	}
	return strings.Join(amountsStrVec[:], ",")
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

func (srv *Server) issue_order(amounts [4]int) {
	for i := 0; i < 4; i++ {
		srv.record.Amounts[i] -= amounts[i]
	}
}

func (srv *Server) sufficient_resources(amounts [4]int) bool {

	for i := 0; i < 4; i++ {
		if amounts[i] > srv.record.Amounts[i] {
			return false
		}
	}
	return true
}

func (srv *Server) handle_order(name string, amounts [4]int) {

	if !srv.sufficient_resources(amounts) {
		fmt.Fprintf(os.Stdout, "Cannot place order for %s.\n", name)
		return
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

func (srv *Server) has_rec(entry *LogEntry, recipient string) bool {
	return srv.record.TimeVector[recipient][entry.Site] >= entry.Time
}

func get_sorted_keys(dictionary *map[string]Order) []string {

	names := make([]string, len(*dictionary))
	idx := 0
	for key := range *dictionary {
		names[idx] = key
		idx += 1
	}
	sort.Strings(names)
	return names
}

func (srv *Server) handle_list_orders() {
	names := get_sorted_keys(&srv.record.Dictionary)
	for _, key := range names {
		fmt.Fprintf(os.Stdin, "%s %s %s\n",
			key, amountsStr(srv.record.Dictionary[key].Amounts),
			strStatus(srv.record.Dictionary[key].Status))
	}
}

func (srv *Server) handle_list_inventory() {
	for idx, val := range item_names {
		fmt.Fprintf(os.Stdin, "%s %d\n",
			val, srv.record.Amounts[idx])
	}
}

func (srv *Server) filter_log(log *[]LogEntry, recipient_id string) []LogEntry {
	ret := make([]LogEntry, 0)
	for _, event := range *log {
		if !srv.has_rec(&event, recipient_id) {
			ret = append(ret, event)
		}
	}
	return ret
}
func (srv *Server) handle_send_to_site_id(site_id string) {
	NP := srv.filter_log(&srv.record.PartialLog, site_id)
	m := Message{NP: NP,
		T:      srv.record.TimeVector,
		Sender: srv.site_id}
	b, err := json.Marshal(&m)
	if err != nil {
		log.Fatalf("Message Marshal error: %v\n", err)
	}
	conn, err := net.Dial("udp", srv.peer_addrs[site_id].String())
	if err != nil {
		log.Fatalf("udp Dial error: %v\n", err)
	}
	_, err = conn.Write(b)
	if err != nil {
		log.Fatalf("udp Write error: %v\n", err)
	}
	fmt.Printf("conn.Close(): %v\n", conn.Close())
}

func (srv *Server) handle_sendall() {
	for site_id := range srv.peers {
		srv.handle_send_to_site_id(site_id)
	}
}

func (srv *Server) handle_list_log() {
	for _, event := range srv.record.PartialLog {
		if event.Type == logOrder {
			fmt.Fprintf(os.Stdin, "order %s %s\n",
				event.Name, amountsStr(event.Amounts))
		} else {
			fmt.Fprintf(os.Stdin, "cancel %s\n", event.Name)
		}
	}
}

func (srv *Server) handle_list_clock() {
	names := get_sorted_keys(&srv.record.Dictionary)
	ret := make([][]int, len(names))

	for i, site_i := range names {
		ret[i] = make([]int, len(names))
		for j, site_j := range names {
			ret[i][j] = srv.record.TimeVector[site_i][site_j]
		}
	}
}

func (srv *Server) filtered_dictionary(NE *[]LogEntry) map[string]Order {
	to_delete := make(map[string]bool)
	for _, event := range *NE {
		if event.Type == logCancel {
			to_delete[event.Name] = true
		}
	}
	result := make(map[string]Order)
	for name, order := range srv.record.Dictionary {
		if _, exists := to_delete[name]; !exists {
			result[name] = order
		}
	}
	for _, event := range *NE {
		if _, exists := to_delete[event.Name]; !exists {
			result[event.Name] = Order{Amounts: event.Amounts, Status: pending}
		}
	}
	return result
}
func (srv *Server) can_delete_event(event *LogEntry) bool {
	for recipient_id := range srv.record.TimeVector {
		if !srv.has_rec(event, recipient_id) {
			return false
		}
	}
	return true
}
func (srv *Server) prune_log(NE *[]LogEntry) ([]LogEntry, []string) {
	deletable := make([]string, 0)
	result := make([]LogEntry, 0)
	for _, event := range srv.record.PartialLog {
		if srv.can_delete_event(&event) {
			deletable = append(deletable, event.Name)
		} else {
			result = append(result, event)
		}
	}

	for _, event := range *NE {
		if srv.can_delete_event(&event) {
			deletable = append(deletable, event.Name)
		} else {
			result = append(result, event)
		}
	}

	return result, deletable
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func (srv *Server) handle_receive(mesg *Message) {

	NE := srv.filter_log(&mesg.NP, srv.site_id)
	srv.record.Dictionary = srv.filtered_dictionary(&NE)

	for r := range srv.record.TimeVector {
		srv.record.TimeVector[srv.site_id][r] =
			max(srv.record.TimeVector[srv.site_id][r],
				mesg.T[mesg.Sender][r])
	}

	for r := range srv.record.TimeVector {
		for s := range srv.record.TimeVector {
			srv.record.TimeVector[r][s] =
				max(srv.record.TimeVector[r][s],
					mesg.T[r][s])
		}
	}

	pruned, deletable := srv.prune_log(&NE)
	srv.record.PartialLog = pruned
	for _, name := range deletable {
		if order, exists := srv.record.Dictionary[name]; exists {
			if srv.sufficient_resources(order.Amounts) {
				srv.issue_order(order.Amounts)
				order.Status = filled
				srv.record.Dictionary[name] = order
			} else {
				srv.handle_cancel(name)
			}
		}
	}

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
