// Kissa Zahra                     i21-0572
// Aliza Ibrahim                   i21-0470
// Hamna Rizwan                    i21-0603
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// state types
const (
	Follower  = "Follower"
	Candidate = "Candidate"
	Leader    = "Leader"
)

// Commands setting for key bval storage
const (
	PUT_CMD    = "PUT"
	APPEND_CMD = "APPEND"
	GET_CMD    = "GET"
)

// command for the key-value store
type KVCommand struct {
	Type  string `json:"type"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

// results
type KVResult struct {
	Success bool   `json:"success"`
	Value   string `json:"value,omitempty"`
	Error   string `json:"error,omitempty"`
}

type LogEntry struct {
	Term    int       `json:"term"`
	Command KVCommand `json:"command"`
	Index   int       `json:"index"`
	Result  *KVResult `json:"result,omitempty"`
}

type Node struct {
	id              int
	state           string
	peers           []int
	currentTerm     int
	votedFor        int
	log             []LogEntry
	commitIndex     int
	lastApplied     int
	nextIndex       map[int]int
	matchIndex      map[int]int
	votesReceived   int
	leaderID        int
	electionTimeout time.Duration
	heartbeatTimer  *time.Timer
	mutex           sync.Mutex
	partitioned     bool
	kvStore         map[string]string
	pendingCmds     map[int]chan KVResult
	cmdResultMtx    sync.RWMutex
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// Declaring Globally
var (
	nodes        []*Node
	networkMutex sync.Mutex
)

func NewNode(id int, peers []int) *Node {
	node := &Node{
		id:              id,
		state:           Follower,
		peers:           peers,
		currentTerm:     0,
		votedFor:        -1,
		log:             make([]LogEntry, 0),
		commitIndex:     0,
		lastApplied:     0,
		nextIndex:       make(map[int]int),
		matchIndex:      make(map[int]int),
		leaderID:        -1,
		electionTimeout: time.Duration(300+rand.Intn(150)) * time.Millisecond,
		partitioned:     false,
		kvStore:         make(map[string]string),
		pendingCmds:     make(map[int]chan KVResult),
	}
	node.log = append(node.log, LogEntry{
		Term:    0,
		Command: KVCommand{Type: "", Key: "", Value: ""},
		Index:   0,
	})
	node.resetElectionTimer()
	return node
}

func (n *Node) resetElectionTimer() {
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}
	n.heartbeatTimer = time.AfterFunc(n.electionTimeout, func() {
		n.startElection()
	})
}

func (n *Node) startElection() {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	if n.state == Leader || n.partitioned {
		return
	}

	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.votesReceived = 1
	n.leaderID = -1

	log.Printf("Node %d: Starting election for term %d\n", n.id, n.currentTerm)

	lastLogIndex := len(n.log) - 1
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = n.log[lastLogIndex].Term
	}

	args := RequestVoteArgs{
		Term:         n.currentTerm,
		CandidateID:  n.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// getting votes
	for _, peer := range n.peers {
		go func(peerID int) {
			if !canCommunicate(n.id, peerID) {
				return
			}

			reply := RequestVoteReply{}
			nodes[peerID].RequestVote(&args, &reply)

			n.mutex.Lock()
			defer n.mutex.Unlock()

			if n.state != Candidate {
				return
			}

			if reply.Term > n.currentTerm {
				n.currentTerm = reply.Term
				n.state = Follower
				n.votedFor = -1
				n.resetElectionTimer()
				return
			}

			if reply.VoteGranted {
				n.votesReceived++
				if n.votesReceived > (len(nodes)/2) && n.state == Candidate {
					n.becomeLeader()
				}
			}
		}(peer)
	}

	n.resetElectionTimer()
}

func (n *Node) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	reply.Term = n.currentTerm
	reply.VoteGranted = false

	if args.Term < n.currentTerm {
		return
	}

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.state = Follower
		n.votedFor = -1
		n.resetElectionTimer()
	}

	// is voting done?
	lastLogIndex := len(n.log) - 1
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = n.log[lastLogIndex].Term
	}

	logOk := (args.LastLogTerm > lastLogTerm) ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (n.votedFor == -1 || n.votedFor == args.CandidateID) && logOk {
		reply.VoteGranted = true
		n.votedFor = args.CandidateID
		n.resetElectionTimer()
	}
}

func (n *Node) becomeLeader() {
	log.Printf("Node %d: Became leader in term %d\n", n.id, n.currentTerm)
	n.state = Leader
	n.leaderID = n.id

	for _, peer := range n.peers {
		n.nextIndex[peer] = len(n.log)
		n.matchIndex[peer] = 0
	}

	n.sendHeartbeats()
}

func (n *Node) sendHeartbeats() {
	if n.state != Leader {
		return
	}

	for _, peer := range n.peers {
		go func(peerID int) {
			if !canCommunicate(n.id, peerID) {
				return
			}

			n.mutex.Lock()
			prevLogIndex := n.nextIndex[peerID] - 1
			prevLogTerm := 0
			if prevLogIndex >= 0 && prevLogIndex < len(n.log) {
				prevLogTerm = n.log[prevLogIndex].Term
			}

			entries := make([]LogEntry, 0)
			if n.nextIndex[peerID] < len(n.log) {
				entries = n.log[n.nextIndex[peerID]:]
			}

			args := AppendEntriesArgs{
				Term:         n.currentTerm,
				LeaderID:     n.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: n.commitIndex,
			}
			n.mutex.Unlock()

			reply := AppendEntriesReply{}
			nodes[peerID].AppendEntries(&args, &reply)

			n.mutex.Lock()
			defer n.mutex.Unlock()

			if reply.Term > n.currentTerm {
				n.currentTerm = reply.Term
				n.state = Follower
				n.votedFor = -1
				n.resetElectionTimer()
				return
			}

			if reply.Success {

				if len(entries) > 0 {
					n.nextIndex[peerID] = args.PrevLogIndex + len(entries) + 1
					n.matchIndex[peerID] = args.PrevLogIndex + len(entries)
				}

				n.updateCommitIndex()
			} else {

				if n.nextIndex[peerID] > 1 {
					n.nextIndex[peerID]--
				}
			}
		}(peer)
	}

	time.AfterFunc(150*time.Millisecond, func() {
		n.sendHeartbeats()
	})
}

func (n *Node) updateCommitIndex() {
	if n.state != Leader {
		return
	}

	for i := n.commitIndex + 1; i < len(n.log); i++ {
		if n.log[i].Term != n.currentTerm {
			continue
		}

		count := 1
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= i {
				count++
			}
		}

		if count > len(nodes)/2 {
			n.commitIndex = i
			n.applyCommittedEntries()
		} else {
			break
		}
	}
}

func (n *Node) applyCommittedEntries() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied]

		result := n.applyToStateMachine(&entry)
		n.log[n.lastApplied].Result = &result

		if n.state == Leader {
			n.cmdResultMtx.RLock()
			if ch, ok := n.pendingCmds[entry.Index]; ok {
				ch <- result
				delete(n.pendingCmds, entry.Index)
			}
			n.cmdResultMtx.RUnlock()
		}

		log.Printf("Node %d: Applied entry %d: %s %s=%s, result=%v\n",
			n.id, n.lastApplied, entry.Command.Type, entry.Command.Key, entry.Command.Value, result.Success)
	}
}

func (n *Node) applyToStateMachine(entry *LogEntry) KVResult {
	cmd := entry.Command
	result := KVResult{Success: true}

	switch cmd.Type {
	case PUT_CMD:
		n.kvStore[cmd.Key] = cmd.Value

	case APPEND_CMD:
		currentVal, exists := n.kvStore[cmd.Key]
		if exists {
			n.kvStore[cmd.Key] = currentVal + cmd.Value
		} else {
			n.kvStore[cmd.Key] = cmd.Value
		}

	case GET_CMD:
		val, exists := n.kvStore[cmd.Key]
		if exists {
			result.Value = val
		} else {
			result.Success = false
			result.Error = "Key not found"
		}

	default:
		result.Success = false
		result.Error = "Unknown command type"
	}

	return result
}

func (n *Node) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	reply.Success = false
	reply.Term = n.currentTerm

	if args.Term < n.currentTerm {
		return
	}
	n.resetElectionTimer()

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.state = Follower
		n.votedFor = -1
	}
	n.leaderID = args.LeaderID
	n.state = Follower

	if args.PrevLogIndex >= len(n.log) ||
		(args.PrevLogIndex > 0 && n.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		return
	}

	newEntriesIndex := 0

	for i := args.PrevLogIndex + 1; i < len(n.log) && newEntriesIndex < len(args.Entries); i++ {
		if i >= len(n.log) || n.log[i].Term != args.Entries[newEntriesIndex].Term {
			n.log = n.log[:i]
			break
		}
		newEntriesIndex++
	}

	for ; newEntriesIndex < len(args.Entries); newEntriesIndex++ {
		entry := args.Entries[newEntriesIndex]
		n.log = append(n.log, entry)
	}

	if args.LeaderCommit > n.commitIndex {
		newCommitIndex := args.LeaderCommit
		if args.LeaderCommit > len(n.log)-1 {
			newCommitIndex = len(n.log) - 1
		}
		n.commitIndex = newCommitIndex
		n.applyCommittedEntries()
	}

	reply.Success = true
}

func (n *Node) ExecuteCommand(cmd KVCommand) (KVResult, bool) {
	n.mutex.Lock()
	if n.state != Leader {
		leaderID := n.leaderID
		n.mutex.Unlock()

		if leaderID != -1 && leaderID >= 0 && leaderID < len(nodes) {
			log.Printf("Node %d: Redirecting %s command to leader (Node %d)\n",
				n.id, cmd.Type, leaderID)
			return nodes[leaderID].ExecuteCommand(cmd)
		}

		return KVResult{Success: false, Error: "No leader available"}, false
	}

	if cmd.Type == GET_CMD {
		val, exists := n.kvStore[cmd.Key]
		n.mutex.Unlock()

		if exists {
			return KVResult{Success: true, Value: val}, true
		}

	} else {
		n.mutex.Unlock()
	}

	n.mutex.Lock()
	newEntry := LogEntry{
		Term:    n.currentTerm,
		Command: cmd,
		Index:   len(n.log),
	}

	resultCh := make(chan KVResult, 1)
	n.cmdResultMtx.Lock()
	n.pendingCmds[newEntry.Index] = resultCh
	n.cmdResultMtx.Unlock()

	n.log = append(n.log, newEntry)
	n.matchIndex[n.id] = len(n.log) - 1

	log.Printf("Node %d: Added %s command to log: %s=%s (index %d)\n",
		n.id, cmd.Type, cmd.Key, cmd.Value, newEntry.Index)

	n.mutex.Unlock()

	select {
	case result := <-resultCh:
		return result, true
	case <-time.After(5 * time.Second):
		return KVResult{Success: false, Error: "Command timed out"}, false
	}
}
func (n *Node) Put(key, value string) (KVResult, bool) {
	return n.ExecuteCommand(KVCommand{
		Type:  PUT_CMD,
		Key:   key,
		Value: value,
	})
}

func (n *Node) Append(key, value string) (KVResult, bool) {
	return n.ExecuteCommand(KVCommand{
		Type:  APPEND_CMD,
		Key:   key,
		Value: value,
	})
}

func (n *Node) Get(key string) (KVResult, bool) {
	return n.ExecuteCommand(KVCommand{
		Type:  GET_CMD,
		Key:   key,
		Value: "",
	})
}

func GetCurrentLeader() int {
	for _, node := range nodes {
		if node.state == Leader {
			return node.id
		}
	}
	return -1
}

func canCommunicate(from, to int) bool {
	networkMutex.Lock()
	defer networkMutex.Unlock()

	return !nodes[from].partitioned && !nodes[to].partitioned
}

func togglePartition(nodeID int) {
	networkMutex.Lock()
	defer networkMutex.Unlock()

	nodes[nodeID].partitioned = !nodes[nodeID].partitioned
	status := "partitioned"
	if !nodes[nodeID].partitioned {
		status = "reconnected"
	}
	log.Printf("Node %d: %s from network\n", nodeID, status)
}

func resetPartitions() {
	networkMutex.Lock()
	defer networkMutex.Unlock()

	for _, node := range nodes {
		if node.partitioned {
			node.partitioned = false
			log.Printf("Node %d: reconnected to network\n", node.id)
		}
	}
}

func getNodeState(nodeID int) string {
	nodes[nodeID].mutex.Lock()
	defer nodes[nodeID].mutex.Unlock()

	return fmt.Sprintf("Node %d: State=%s, Term=%d, Leader=%d, Log=%d entries, Committed=%d",
		nodeID, nodes[nodeID].state, nodes[nodeID].currentTerm, nodes[nodeID].leaderID,
		len(nodes[nodeID].log), nodes[nodeID].commitIndex)
}

func getKVStore(nodeID int) string {
	nodes[nodeID].mutex.Lock()
	defer nodes[nodeID].mutex.Unlock()

	result := fmt.Sprintf("KV Store for Node %d:\n", nodeID)
	if len(nodes[nodeID].kvStore) == 0 {
		result += "  (empty)\n"
	} else {
		for k, v := range nodes[nodeID].kvStore {
			result += fmt.Sprintf("  %s: %s\n", k, v)
		}
	}
	return result
}

func startCLI() {
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("\nRaft Key-Value Store CLI")
	fmt.Println("=======================")
	fmt.Println("put <key> <value>   - Store a key-value pair")
	fmt.Println("append <key> <value> - Append value to existing key")
	fmt.Println("get <key>           - Retrieve a value by key")
	fmt.Println("store [id]          - Show KV store contents for a node (or all if not specified)")
	fmt.Println("status              - Show status of all nodes")
	fmt.Println("leader              - Show current leader")
	fmt.Println("partition <id>      - Toggle network partition for a node")
	fmt.Println("reset               - Reset all network partitions")
	fmt.Println("kill <id>           - Simulate a node crash (partition + restart)")
	fmt.Println("logs <id>           - Show logs for a specific node")
	fmt.Println("exit                - Exit the program")
	fmt.Println()

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		cmd := scanner.Text()
		parts := strings.Fields(cmd)

		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "put":
			if len(parts) < 3 {
				fmt.Println("Usage: put <key> <value>")
				continue
			}

			key := parts[1]
			value := strings.Join(parts[2:], " ")

			leaderID := GetCurrentLeader()
			if leaderID == -1 {
				fmt.Println("No leader currently available")
				continue
			}

			result, ok := nodes[leaderID].Put(key, value)
			if ok && result.Success {
				fmt.Printf("Successfully stored %s=%s\n", key, value)
			} else {
				fmt.Printf("Failed to store value: %s\n", result.Error)
			}

		case "append":
			if len(parts) < 3 {
				fmt.Println("Usage: append <key> <value>")
				continue
			}

			key := parts[1]
			value := strings.Join(parts[2:], " ")

			leaderID := GetCurrentLeader()
			if leaderID == -1 {
				fmt.Println("No leader currently available")
				continue
			}

			result, ok := nodes[leaderID].Append(key, value)
			if ok && result.Success {
				fmt.Printf("Successfully appended to %s\n", key)
			} else {
				fmt.Printf("Failed to append value: %s\n", result.Error)
			}

		case "get":
			if len(parts) < 2 {
				fmt.Println("Usage: get <key>")
				continue
			}

			key := parts[1]

			leaderID := GetCurrentLeader()
			if leaderID == -1 {
				fmt.Println("No leader currently available")
				continue
			}

			result, ok := nodes[leaderID].Get(key)
			if ok && result.Success {
				fmt.Printf("%s = %s\n", key, result.Value)
			} else {
				fmt.Printf("Failed to get value: %s\n", result.Error)
			}

		case "store":
			if len(parts) > 1 {
				nodeID, err := strconv.Atoi(parts[1])
				if err != nil || nodeID < 0 || nodeID >= len(nodes) {
					fmt.Println("Invalid node ID")
					continue
				}
				fmt.Println(getKVStore(nodeID))
			} else {
				for i := range nodes {
					fmt.Println(getKVStore(i))
				}
			}

		case "status":
			for i := range nodes {
				fmt.Println(getNodeState(i))
			}

		case "leader":
			leaderID := GetCurrentLeader()
			if leaderID == -1 {
				fmt.Println("No leader currently available")
			} else {
				fmt.Printf("Current leader is Node %d\n", leaderID)
			}

		case "partition":
			if len(parts) < 2 {
				fmt.Println("Please specify node ID")
				continue
			}

			nodeID, err := strconv.Atoi(parts[1])
			if err != nil || nodeID < 0 || nodeID >= len(nodes) {
				fmt.Println("Invalid node ID")
				continue
			}

			togglePartition(nodeID)
			fmt.Printf("Toggled partition status for Node %d\n", nodeID)

		case "reset":
			resetPartitions()
			fmt.Println("All network partitions have been reset")

		case "kill":
			if len(parts) < 2 {
				fmt.Println("Please specify node ID")
				continue
			}

			nodeID, err := strconv.Atoi(parts[1])
			if err != nil || nodeID < 0 || nodeID >= len(nodes) {
				fmt.Println("Invalid node ID")
				continue
			}

			togglePartition(nodeID)
			fmt.Printf("Node %d crashed\n", nodeID)

			go func(id int) {
				time.Sleep(2 * time.Second)
				nodes[id].mutex.Lock()
				nodes[id].state = Follower
				nodes[id].currentTerm = nodes[id].currentTerm + 1
				nodes[id].votedFor = -1
				nodes[id].resetElectionTimer()

				nodes[id].mutex.Unlock()
				togglePartition(id)
				fmt.Printf("Node %d restarted\n", id)
			}(nodeID)

		case "logs":
			if len(parts) < 2 {
				fmt.Println("Please specify node ID")
				continue
			}

			nodeID, err := strconv.Atoi(parts[1])
			if err != nil || nodeID < 0 || nodeID >= len(nodes) {
				fmt.Println("Invalid node ID")
				continue
			}

			nodes[nodeID].mutex.Lock()
			fmt.Printf("Logs for Node %d:\n", nodeID)
			for i, entry := range nodes[nodeID].log {
				if i == 0 { //skipping the first entry as it is dummy
					continue
				}
				fmt.Printf("  Index %d: Term %d, Command: %s %s=%s\n",
					entry.Index, entry.Term, entry.Command.Type, entry.Command.Key, entry.Command.Value)
			}
			fmt.Printf("Commit Index: %d\n", nodes[nodeID].commitIndex)
			nodes[nodeID].mutex.Unlock()

		case "exit":
			fmt.Println("Exiting...")
			return

		default:
			fmt.Println("Unknown command. Type 'help' for available commands.")
		}
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	numNodes := 5
	if len(os.Args) > 1 {
		if n, err := strconv.Atoi(os.Args[1]); err == nil && n > 0 {
			numNodes = n
		}
	}

	nodes = make([]*Node, numNodes)
	for i := 0; i < numNodes; i++ {
		peers := make([]int, 0)
		for j := 0; j < numNodes; j++ {
			if i != j {
				peers = append(peers, j)
			}
		}
		nodes[i] = NewNode(i, peers)
	}

	go func() {
		http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
			for _, node := range nodes {
				node.mutex.Lock()
				fmt.Fprintf(w, "Node %d - State: %s, Term: %d, Leader: %d, Log Entries: %d, Committed: %d\n",
					node.id, node.state, node.currentTerm, node.leaderID, len(node.log), node.commitIndex)
				node.mutex.Unlock()
			}
		})

		http.HandleFunc("/kv", func(w http.ResponseWriter, r *http.Request) {
			leaderID := GetCurrentLeader()
			if leaderID == -1 {
				fmt.Fprintf(w, "No leader available\n")
				return
			}

			nodes[leaderID].mutex.Lock()
			kvData, _ := json.MarshalIndent(nodes[leaderID].kvStore, "", "  ")
			nodes[leaderID].mutex.Unlock()

			w.Header().Set("Content-Type", "application/json")
			w.Write(kvData)
		})

		log.Printf("HTTP server started at :8080\n")
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	log.Printf("Raft cluster started with %d nodes\n", numNodes)
	startCLI()
}
