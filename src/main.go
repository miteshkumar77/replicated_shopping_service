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
)

// largest udp packet data size
const max_pkt_size = 1024

// largest dictionary+log+clock json dump size
const max_dump_size = 1000000

var item_names = [4]string{
	"surgical masks",
	"hand sanitizer bottles",
	"toilet paper rolls",
	"reese’s peanut butter cups"}

var original_amounts = [4]int{500, 100, 200, 200}

// StatusCode is the code for a order status in the dictionary
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

// LogEntryType is the op-code for the log event
// logOrder <-> insert(x); logCancel <-> delete(x)
type LogEntryType int

const (
	logOrder LogEntryType = iota
	logCancel
)

// Node is a struct used for unmarshalling networking
// configurations from knownhosts.json for a single node
type Node struct {
	TcpStartPort int    `json:"tcp_start_port"`
	TcpEndPort   int    `json:"tcp_end_port"`
	UdpStartPort int    `json:"udp_start_port"`
	UdpEndPort   int    `json:"udp_end_port"`
	IpAddress    string `json:"ip_address"`
}

// Map is a struct used for unmarshalling networking
// configurations from knownhosts.json for all nodes
type Map struct {
	Hosts map[string]Node `json:"hosts"`
}

// Order is a dictionary entry in the wuu-bernstein algorithm
type Order struct {
	Amounts [4]int     `json:"amounts"`
	Status  StatusCode `json:"status"`
}

// LogEntry is an event in the log for the wuu-bernstein algorithm
type LogEntry struct {
	Type       LogEntryType   `json:"type"`
	Name       string         `json:"name"`
	Amounts    [4]int         `json:"amounts"`
	Time       int            `json:"time"`
	Site       string         `json:"site"`
	VectorTime map[string]int `json:"vector_time"`
}

// LogRecord stores all state required by the wuu-bernstein algorithm
// and can be loaded/unloaded from a json dump.
type LogRecord struct {
	Dictionary map[string]Order          `json:"dictionary"`
	PartialLog []LogEntry                `json:"partial_log"`
	TimeVector map[string]map[string]int `json:"time_vector"`
	Amounts    [4]int                    `json:"amounts"`
}

// newLogRecord creates a blank LogRecord struct.
// Is meant to be used when a dump file doesn't already exist.
func newLogRecord(peers map[string]Node) *LogRecord {
	TimeVector := make(map[string]map[string]int)
	for site_i := range peers {
		TimeVector[site_i] = make(map[string]int)
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

// Message is the format of the messages that
// are exchanged by the nodes via UDP
type Message struct {
	NP     []LogEntry                `json:"np"`
	T      map[string]map[string]int `json:"t"`
	Sender string                    `json:"sender"`
}

// Server is instantiated and run by each node participating
// in the wuu-bernstein algorithm. It encapsulates the
// LogRecord, standard input listener, udp network listener,
// and the UDP addressing information. Contains handlers
// for all of the different UI components.
type Server struct {
	site_id    string
	peers      map[string]Node
	peer_addrs map[string]*net.UDPAddr
	stdin_c    chan string
	netwk_c    chan Message
	record     LogRecord
}

// amountsStr converts an array of 4 integers representing
// the inventory to a comma delimited string.
func amountsStr(amounts [4]int) string {
	var amountsStrVec [4]string
	for i := 0; i < 4; i++ {
		amountsStrVec[i] = fmt.Sprint(amounts[i])
	}
	return strings.Join(amountsStrVec[:], ",")
}

// read stable storage
func read_stable_storage() *LogRecord {
	record_file, err := os.Open("stable_storage.json")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Open error: %v\n", err)
		return nil
	}
	byteArr, err := ioutil.ReadAll(record_file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading stable_storage.json: %v\n", err)
		return nil
	}
	var record LogRecord
	err = json.Unmarshal(byteArr, &record)
	if err != nil {
		fmt.Fprintf(os.Stderr, "stable_storage unmarshal error: %v\n", err)
		return nil
	}
	record_file.Close()
	return &record
}

// newServer creates a new Server object for a particular site_id.
// If it finds a "stable_storage.json" file, it will load the
// LogRecord contents from there, otherwise it will create
// a blank LogRecord object.
func newServer(site_id string, peers Map) *Server {
	record := read_stable_storage()
	if record == nil {
		record = newLogRecord(peers.Hosts)
	}

	s := Server{site_id: site_id,
		peers:      peers.Hosts,
		peer_addrs: make(map[string]*net.UDPAddr),
		stdin_c:    make(chan string),
		netwk_c:    make(chan Message),
		record:     *record}

	for id, node := range s.peers {
		s.peer_addrs[id] = &net.UDPAddr{
			Port: node.UdpStartPort,
			IP:   net.ParseIP(node.IpAddress)}
	}
	return &s
}

// stdin_read_loop infinitely loops while polling the stdin
// file descriptor for user input, and passing that to the
// user channel
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

// netwk_read_loop infinitely loops while polling a UDP socket
// for messages from other nodes. Upon receiving, it will push
// unmarshal and then push messages to a network message
// designated channel
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

// parse a comma separated list of integers
// into an array of ints
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

// clock_tick increments the site's clock
func (srv *Server) clock_tick() {
	srv.record.TimeVector[srv.site_id][srv.site_id]++
}

// curr_time returns the site's clock value
func (srv *Server) curr_time() int {
	return srv.record.TimeVector[srv.site_id][srv.site_id]
}

// issue_order decrements all elements in an inventory
// by the corresponding values in amounts [4]int.
// Assumes that this order has already been set to the
// 'filled' state.
func (srv *Server) issue_order(amounts [4]int) {
	for i := 0; i < 4; i++ {
		srv.record.Amounts[i] -= amounts[i]
	}
}

// sufficient_resources checks if the current inventory
// has sufficient resources for this order with respect
// to the local node's inventory only
func (srv *Server) sufficient_resources(amounts [4]int) bool {

	for i := 0; i < 4; i++ {
		if amounts[i] > srv.record.Amounts[i] {
			return false
		}
	}
	return true
}

func (srv *Server) vector_time() map[string]int {
	ret := make(map[string]int)
	for k, v := range srv.record.TimeVector[srv.site_id] {
		ret[k] = v
	}
	return ret
}

// handle_order processes an order request locally.
// It is called when a user inputs the 'order' command.
// 1. Checks if sufficient_resources() is true
// 2. Adds a 'pending' order to the local dictionary
// 3. Appends 'logOrder' (insert(x)) event to the log
// 4. Dumps LogRecord contents to stable storage.
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
			Type:       logOrder,
			Name:       name,
			Amounts:    amounts,
			Time:       srv.curr_time(),
			Site:       srv.site_id,
			VectorTime: srv.vector_time()})

	srv.dump_to_stable_storage()

	fmt.Fprintf(os.Stdout, "Order submitted for %s.\n", name)
}

// handle_cancel processes a cancel request locally
// It is called when a user inputs the 'cancel' command.
// 1. Checks if the order entry in the dictionary isn't 'filled'
// 2. Appends a 'logCancel' (delete(x)) event to the log
// 3. Removes the 'pending' order from the local dictionary
// 4. Dumps LogRecord contents to stable_storage.
func (srv *Server) handle_cancel(name string) {

	if srv.record.Dictionary[name].Status == filled {
		fmt.Fprintf(os.Stdout, "Cannot cancel order for %s.\n", name)
		return
	}
	srv.clock_tick()
	srv.record.PartialLog = append(srv.record.PartialLog,
		LogEntry{
			Type:       logCancel,
			Name:       name,
			Amounts:    [4]int{-1, -1, -1, -1},
			Time:       srv.curr_time(),
			Site:       srv.site_id,
			VectorTime: srv.vector_time()})
	delete(srv.record.Dictionary, name)
	srv.dump_to_stable_storage()
	fmt.Fprintf(os.Stdout, "Order for %s cancelled.\n", name)
}

// has_rec is the has_rec helper function described in the wuu-bernstein algorithm
// It checks whether the local site knows if a recipient site already has
// a particular event in its log.
func (srv *Server) has_rec(entry *LogEntry, recipient string) bool {
	return srv.record.TimeVector[recipient][entry.Site] >= entry.Time
}

// return the keys of the dictionary sorted lexicographically.
// i.e. the names of the Orders in the sites dictionary.
// Used in printing out elements for the 'orders' command.
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

// return the keys of the matrix clock sorted lexicographically.
// Used in printing out the matrix clock for the 'clock' command.
func get_sorted_keys_time_vector(TimeVector *map[string]map[string]int) []string {

	names := make([]string, len(*TimeVector))
	idx := 0
	for key := range *TimeVector {
		names[idx] = key
		idx += 1
	}
	sort.Strings(names)
	return names
}

// handle_list_orders is used to handle the 'orders' command.
// Lists out the dictionary contents lexicographically ordered
// by the Name on the order.
func (srv *Server) handle_list_orders() {
	names := get_sorted_keys(&srv.record.Dictionary)
	for _, key := range names {
		fmt.Fprintf(os.Stdout, "%s %s %s\n",
			key, amountsStr(srv.record.Dictionary[key].Amounts),
			strStatus(srv.record.Dictionary[key].Status))
	}
}

// handle_list_inventory is used to handle the 'inventory' command.
// Lists the inventory contents.
func (srv *Server) handle_list_inventory() {
	for idx, val := range item_names {
		fmt.Fprintf(os.Stdout, "%s %d\n",
			val, srv.record.Amounts[idx])
	}
}

// filter_log is a helper function for sending/receiving messages.
// For an input recipient_id and a Log, it filters out events from the log
// that the local site knows the recipient already has.
func (srv *Server) filter_log(log *[]LogEntry, recipient_id string) []LogEntry {
	ret := make([]LogEntry, 0)
	for _, event := range *log {
		if !srv.has_rec(&event, recipient_id) {
			ret = append(ret, event)
		}
	}
	return ret
}

// handle_send_to_site_id handles the 'send' command.
// Filters the log and then sends it and the matrix clock
// via UDP to the recipient site.
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
	conn.Close()
}

// handle_sendall handles the 'sendall' command.
// Simply calls handle_send_to_site_id() for all site_ids
func (srv *Server) handle_sendall() {
	for site_id := range srv.peers {
		srv.handle_send_to_site_id(site_id)
	}
}

// handle_list_log handles the 'log' command.
// Prints out all of the log contents.
func (srv *Server) handle_list_log() {
	for _, event := range srv.record.PartialLog {
		if event.Type == logOrder {
			fmt.Fprintf(os.Stdout, "order %s %s\n",
				event.Name, amountsStr(event.Amounts))
		} else {
			fmt.Fprintf(os.Stdout, "cancel %s\n", event.Name)
		}
	}
}

// handle_list_clock handles the 'clock' command.
// Prints the clock as an NxN matrix where matrix[i][j]
// is the matrix_clock[site_i][site_j] if
// site_i = sites[i], site_j = sites[j] and sites is
// an array of N sites ordered lexicographically.
func (srv *Server) handle_list_clock() {
	names := get_sorted_keys_time_vector(&srv.record.TimeVector)
	clk := make([][]string, len(names))
	for i, site_i := range names {
		clk[i] = make([]string, len(names))
		for j, site_j := range names {
			clk[i][j] = fmt.Sprintf("%d",
				srv.record.TimeVector[site_i][site_j])
		}
	}
	for _, row := range clk {
		fmt.Fprintln(os.Stdout, strings.Join(row, " "))
	}
}

// filtered_dictionary takes a Log that is meant to be
// appended to the server's local log, and returns a new
// filtered version of the local server's dictionary that
// such that no elements in it have a corresponding
// logCancel event in the Log to be appended.
// We assume that the new log, and our existing log are totally ordered
// but all the all events are concurrent across the two
// so we merge them in alphabetical order.
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

// can_delete_event is a helper function for log truncation.
// For a log event, it returns whether the site can say for
// sure that all other sites have the event in their logs.
func (srv *Server) can_delete_event(event *LogEntry) bool {
	for recipient_id := range srv.record.TimeVector {
		if !srv.has_rec(event, recipient_id) {
			return false
		}
	}
	return true
}

func compare_vector_clock(a *map[string]int, b *map[string]int) bool {
	neq := false
	for k, v := range *a {
		if v > (*b)[k] {
			return false
		}
		neq = neq || v != (*b)[k]
	}
	return neq
}

func compare_log_entry(a *LogEntry, b *LogEntry) bool {
	return compare_vector_clock(&a.VectorTime, &b.VectorTime) ||
		(!compare_vector_clock(&b.VectorTime, &a.VectorTime) && a.Name < b.Name)
}

func merge_log(a *[]LogEntry, b *[]LogEntry) []LogEntry {
	i := 0
	j := 0
	n := len(*a)
	m := len(*b)

	res := make([]LogEntry, n+m)
	for i < n || j < m {
		if j >= m || (i < n && compare_log_entry(&(*a)[i], &(*b)[j])) {
			res[i+j] = (*a)[i]
			i++
		} else {
			res[i+j] = (*b)[j]
			j++
		}
	}
	return res
}

func merge_deleted(a *[]LogEntry, b *[]LogEntry) []string {
	tmp := merge_log(a, b)
	ret := make([]string, len(tmp))
	for idx, el := range tmp {
		ret[idx] = el.Name
	}
	return ret
}

// prune_log is a helper function for log truncation.
// Given a new log to be combined with the server's existing log
// It returns the merged log with all deletable events removed.
// It also returns The merged list of names of all of the log entries that were deleted.
// this additional list of names is used to set the status of
// some 'pending' orders to filled.
// 1. Get all un-deletable events as well as the names of all deletable events,
// from the server's original partial log, while maintaining original order
// 2. Get all un-deletable events as well as the names of all deletable events,
// from the log to append, while maintaining original order
// 3. merge the corresponding lists with respect to the lexicographical ordering
//    of the name
func (srv *Server) prune_log(NE *[]LogEntry) ([]LogEntry, []string) {
	deletable_srv := make([]LogEntry, 0)
	result_srv := make([]LogEntry, 0)
	for _, event := range srv.record.PartialLog {
		if srv.can_delete_event(&event) {
			deletable_srv = append(deletable_srv, event)
		} else {
			result_srv = append(result_srv, event)
		}
	}

	deletable_recv := make([]LogEntry, 0)
	result_recv := make([]LogEntry, 0)
	for _, event := range *NE {
		if srv.can_delete_event(&event) {
			deletable_recv = append(deletable_recv, event)
		} else {
			result_recv = append(result_recv, event)
		}
	}

	return merge_log(&result_srv, &result_recv),
		merge_deleted(&deletable_srv, &deletable_recv)
}

// For the current dictionary on the server
// calculate the inventory values.
func (srv *Server) calc_iventory() [4]int {
	res := [4]int{500, 100, 200, 200}
	for _, value := range srv.record.Dictionary {
		if value.Status == filled {
			for idx := 0; idx < 4; idx++ {
				res[idx] -= value.Amounts[idx]
			}
		}
	}
	return res
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

// handle_receive handles a receive of a message from another site.
// 1. Filter the received log to contain only events that we don't have
// 2. Filter our own dictionary based on the delete events in this received log
// 3. Recalculate inventory (we may have deleted some 'filled' orders, in which case
// those resources must be replenished)
// 4. Compute the updated matrix clock as done in the wuu-bernstein algorithm.
// 5. Prune the log (Log Truncation)
// 6. For all orders that are deletable (i.e. all other sites know of their existence)
// And are not already deleted from previous steps: if we have sufficient resources
// apply the order (decrement its resources), otherwise cancel this order
// i.e. handle_cancel(Order Name)
// 7. Dump to stable storage.

func (srv *Server) handle_receive(mesg *Message) {

	NE := srv.filter_log(&mesg.NP, srv.site_id)
	srv.record.Dictionary = srv.filtered_dictionary(&NE)
	srv.record.Amounts = srv.calc_iventory()

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

	srv.dump_to_stable_storage()

}

func (srv *Server) dump_to_stable_storage() {
	record_file, err := os.OpenFile("stable_storage.json",
		os.O_TRUNC|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		log.Fatalf("stable_storage.json open error: %v\n", err)
	}

	b, err := json.Marshal(&srv.record)
	if err != nil {
		log.Fatalf("server record marshal error: %v\n", err)
	}
	_, err = record_file.Write(b)
	if err != nil {
		log.Fatalf("stable_storage.json write error: %v\n", err)
	}
	record_file.Close()
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
		os.Exit(0)
		// return // EXIT POINT
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
	var peers Map
	err = json.Unmarshal(byteArr, &peers)
	if err != nil {
		log.Fatalf("Error unmarshalling in main: %v\n", err)
	}
	s := newServer(site_id, peers)
	s.run()
}
