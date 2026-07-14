package node

import (
	"github.com/google/uuid"
	"github.com/nikitakosatka/hive/pkg/hive"
	"maps"
	"sync"
	"time"
)

const (
	metaMsgID    = "metaMsgID"
	metaClock    = "metaClock"
	refloodDelay = 100 * time.Millisecond
)

// BroadcastNode is a node that supports reliable causal broadcast.
type BroadcastNode interface {
	hive.Node
	Broadcast(payload interface{}) error
	DeliveredMessages() []hive.Message
}

type ReliableCausalBroadcastNode struct {
	*hive.BaseNode

	mu sync.Mutex

	sendSeq    int
	allNodeIDs []string // ids of all available nodes

	receivedMessageIDs map[string]struct{} // deduplication
	deliveredClock     map[string]int

	deliveredMessages []*hive.Message // received messages that are ordered
	bufferedMessages  []*hive.Message // received messages that cant be ordered yet
}

// NewReliableCausalBroadcastNode creates a new node that performs reliable causal broadcast.
func NewReliableCausalBroadcastNode(id string, allNodeIDs []string) BroadcastNode {
	clock := make(map[string]int)
	for _, nodeId := range allNodeIDs {
		clock[nodeId] = 0
	}

	node := &ReliableCausalBroadcastNode{
		BaseNode: hive.NewBaseNode(id),
		mu:       sync.Mutex{},

		sendSeq:    0,
		allNodeIDs: allNodeIDs,

		receivedMessageIDs: make(map[string]struct{}),
		deliveredClock:     clock,

		deliveredMessages: []*hive.Message{},
		bufferedMessages:  []*hive.Message{},
	}

	go node.refloodReceived()
	return node
}

func (n *ReliableCausalBroadcastNode) refloodReceived() {
	for {
		n.mu.Lock()
		for _, msg := range n.deliveredMessages {
			n.floodMessage(msg)
		}
		n.mu.Unlock()

		time.Sleep(refloodDelay)
	}
}

func (n *ReliableCausalBroadcastNode) floodMessage(msg *hive.Message) {
	for _, to := range n.allNodeIDs {
		floodMsg := hive.NewMessage(n.ID(), to, msg.Payload)
		floodMsg.Metadata = msg.Metadata
		n.SendMessage(floodMsg)
	}
}

// Broadcast sends the payload to all nodes (including self). Each recipient will
// apply reliable broadcast (flood) and causal delivery.
func (n *ReliableCausalBroadcastNode) Broadcast(payload interface{}) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	var clock = maps.Clone(n.deliveredClock)
	clock[n.ID()] = n.sendSeq

	msgID := uuid.New().String()

	for _, to := range n.allNodeIDs {
		msg := hive.NewMessage(n.ID(), to, payload)
		msg.Metadata[metaMsgID] = msgID
		msg.Metadata[metaClock] = clock
		n.SendMessage(msg)
	}

	n.sendSeq++
	return nil
}

func (n *ReliableCausalBroadcastNode) Send(to string, payload interface{}) error {
	panic("TODO check if method is not used")
	// msg := hive.NewMessage(n.ID(), to, payload)
	// return n.SendMessage(msg)
}

func (n *ReliableCausalBroadcastNode) Receive(msg *hive.Message) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if _, exists := n.receivedMessageIDs[getID(msg)]; exists {
		return nil
	}

	n.floodMessage(msg)
	n.receivedMessageIDs[getID(msg)] = struct{}{}

	n.bufferedMessages = append(n.bufferedMessages, msg)
	n.tryDeliver()
	return nil
}

func (n *ReliableCausalBroadcastNode) tryDeliver() {
	for {
		hasProgress := false

		for i := 0; i < len(n.bufferedMessages); i++ {
			msg := n.bufferedMessages[i]
			if happensBefore(msg.Metadata[metaClock].(map[string]int), n.deliveredClock, true) {
				n.deliveredMessages = append(n.deliveredMessages, msg)
				n.deliveredClock[msg.From]++
				n.bufferedMessages = append(n.bufferedMessages[:i], n.bufferedMessages[i+1:]...)
				hasProgress = true
				break
			}
		}

		if !hasProgress {
			break
		}
	}
}

// DeliveredMessages returns the application-level messages that have been
// delivered to this node, in delivery order.
func (n *ReliableCausalBroadcastNode) DeliveredMessages() []hive.Message {
	n.mu.Lock()
	defer n.mu.Unlock()

	result := make([]hive.Message, 0, len(n.deliveredMessages))
	for _, element := range n.deliveredMessages {
		if element != nil {
			result = append(result, *element)
		}
	}
	return result
}

// Orderer orders events based on their vector clocks and identifies groups of
// mutually parallel events.
type Orderer struct{}

func (o *Orderer) Order(msgs ...hive.Message) (ordered []string, parallel [][]string) {
	vectorClocks := make([]map[string]int, len(msgs))
	payloads := make([]string, len(msgs))
	for i, m := range msgs {
		vectorClocks[i] = m.Metadata[metaClock].(map[string]int)
		payloads[i] = m.Payload.(string)
	}

	used := make([]bool, len(vectorClocks))
	for len(ordered) != len(vectorClocks) {
		var sameLevelIdxs []int
		for i, checkVectorClock := range vectorClocks {
			if used[i] {
				continue
			}

			beforeAll := true
			for j, otherVectorClock := range vectorClocks {
				if used[j] || (i == j) {
					continue
				}

				if happensBefore(otherVectorClock, checkVectorClock, false) {
					beforeAll = false
					break
				}
			}

			if beforeAll {
				sameLevelIdxs = append(sameLevelIdxs, i)
			}
		}

		if len(sameLevelIdxs) > 1 {
			var sameLevelPayloads []string
			for _, idx := range sameLevelIdxs {
				sameLevelPayloads = append(sameLevelPayloads, payloads[idx])
			}
			parallel = append(parallel, sameLevelPayloads)
		}

		for _, idx := range sameLevelIdxs {
			ordered = append(ordered, payloads[idx])
			used[idx] = true
		}
	}

	return
}

func happensBefore(vectorClock1, vectorClock2 map[string]int, orEqual bool) bool {
	less := false
	for k, _ := range vectorClock1 {
		if vectorClock1[k] > vectorClock2[k] {
			return false
		}
		if vectorClock1[k] < vectorClock2[k] || (orEqual && vectorClock1[k] == vectorClock2[k]) {
			less = true
		}
	}
	return less
}

// ===================
// UTILS
// ===================
func getID(m *hive.Message) string {
	return m.Metadata[metaMsgID].(string)
}
