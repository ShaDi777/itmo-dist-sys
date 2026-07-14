package node

import (
	"github.com/google/uuid"
	"github.com/nikitakosatka/hive/pkg/hive"
	"sync"
	"time"
)

const MsgID = "MsgID"
const ACK = "ACK"
const VectorClock = "VectorClock"

type OrderedNode interface {
	hive.Node

	// DeliveredMessages returns all application-level messages that have been
	// delivered to this node. This is used by tests together with the Orderer
	// to verify both reliability and ordering/parallelism properties.
	DeliveredMessages() []hive.Message
}

// ReliableOrderedNode is a reference implementation of OrderedNode.
type ReliableOrderedNode struct {
	*hive.BaseNode

	mu sync.Mutex

	awaitAcks          map[string]*hive.Message
	acceptedMessageIDs map[string]struct{}
	vectorClock        map[string]int
	receivedMessages   []hive.Message
}

// NewReliableOrderedNode creates a new node with an initial zeroed vector
// clock for all known node IDs.
func NewReliableOrderedNode(id string, allNodeIDs []string) OrderedNode {
	vectorClock := make(map[string]int)
	for _, nodeId := range allNodeIDs {
		vectorClock[nodeId] = 0
	}

	ron := &ReliableOrderedNode{
		BaseNode:           hive.NewBaseNode(id),
		mu:                 sync.Mutex{},
		awaitAcks:          make(map[string]*hive.Message),
		acceptedMessageIDs: make(map[string]struct{}),
		vectorClock:        vectorClock,
		receivedMessages:   []hive.Message{},
	}

	go ron.retryAwaiting()
	return ron
}

func (n *ReliableOrderedNode) retryAwaiting() {
	for {
		n.mu.Lock()
		for _, msg := range n.awaitAcks {
			n.SendMessage(msg)
		}
		n.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}
}

// DeliveredMessages returns the application-level messages that have been
// delivered to this node, in delivery order.
func (n *ReliableOrderedNode) DeliveredMessages() []hive.Message {
	return n.receivedMessages
}

func (n *ReliableOrderedNode) Send(to string, payload interface{}) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.vectorClock[n.ID()]++

	copyVectorClock := make(map[string]int)
	for k, v := range n.vectorClock {
		copyVectorClock[k] = v
	}

	msgID := uuid.New().String()
	message := hive.NewMessage(n.ID(), to, payload)
	message.Metadata[MsgID] = msgID
	message.Metadata[ACK] = false
	message.Metadata[VectorClock] = copyVectorClock

	n.awaitAcks[msgID] = message
	return n.SendMessage(message)
}

func (n *ReliableOrderedNode) Receive(msg *hive.Message) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	msgID := msg.Metadata[MsgID].(string)
	ack, ackExists := msg.Metadata[ACK].(bool)

	if ackExists && ack {
		delete(n.awaitAcks, msgID)
		return nil
	}

	if _, exists := n.acceptedMessageIDs[msgID]; !exists {
		msgVectorClock := msg.Metadata[VectorClock].(map[string]int)
		for k, currentValue := range n.vectorClock {
			n.vectorClock[k] = max(currentValue, msgVectorClock[k])
		}
		n.vectorClock[n.ID()]++

		n.receivedMessages = append(n.receivedMessages, *msg)
		n.acceptedMessageIDs[msgID] = struct{}{}
	}

	// send ack
	senderID := msg.From
	ackMsg := hive.NewMessage(n.ID(), senderID, nil)
	ackMsg.Metadata[MsgID] = msg.Metadata[MsgID]
	ackMsg.Metadata[ACK] = true
	n.SendMessage(ackMsg)

	return nil
}

// Orderer orders events based on their vector clocks and identifies groups of
// mutually parallel events.
type Orderer struct{}

func (o *Orderer) Order(msgs ...hive.Message) (ordered []string, parallel [][]string) {
	vectorClocks := make([]map[string]int, len(msgs))
	payloads := make([]string, len(msgs))
	for i, m := range msgs {
		vectorClocks[i] = m.Metadata[VectorClock].(map[string]int)
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

				if happensBefore(otherVectorClock, checkVectorClock) {
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

func happensBefore(vectorClock1, vectorClock2 map[string]int) bool {
	less := false
	for k, _ := range vectorClock1 {
		if vectorClock1[k] > vectorClock2[k] {
			return false
		}
		if vectorClock1[k] < vectorClock2[k] {
			less = true
		}
	}
	return less
}
