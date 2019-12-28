package bully

// msgType represents the type of messages that can be passed between nodes
type msgType int

// customized messages to be used for communication between nodes
const (
	ELECTION msgType = iota
	OK
	COORDINATOR
)

// Message is used to communicate with the other nodes
type Message struct {
	Pid   int
	Round int
	Type  msgType
}

// Bully runs the bully algorithm for leader election on a node
func Bully(pid int, leader int, checkLeader chan bool, comm map[int]chan Message, startRound <-chan int, quit <-chan bool, electionResult chan<- int) {
	startedElection := -100
	checkedLeader := -100
	lastWitnessRound := 0
	for {
		if <-quit {
			return
		}
		roundNum := <-startRound
		select {
		case <-checkLeader:
			comm[leader] <- Message{pid, roundNum, ELECTION}
			checkedLeader = roundNum
			break
		default:
			break
		}
		msgs := getMessages(comm[pid], roundNum-1)
		if pid >= leader && lastWitnessRound != roundNum-1 {
			for i := 0; i <= pid; i++ {
				comm[i] <- Message{pid, roundNum, COORDINATOR}
			}
		}
		for _, m := range msgs {
			if m.Type == ELECTION && m.Pid < pid {
				comm[m.Pid] <- Message{pid, roundNum, OK}
				if startedElection == -100 {
					for i := pid + 1; i <= leader; i++ {
						comm[i] <- Message{pid, roundNum, ELECTION}
					}
					startedElection = roundNum
				}
			}
			if m.Type == OK && m.Pid > pid {
				//stop trynna be leader bro
				if startedElection == roundNum-2 {
					startedElection = -100
				}
				if m.Pid == leader && checkedLeader == roundNum-2 {
					checkedLeader = -100
				}
			}
			if m.Type == COORDINATOR {
				leader = m.Pid
				electionResult <- m.Pid
			}
		}
		if startedElection == roundNum-2 {
			for i := 0; i <= pid; i++ {
				comm[i] <- Message{pid, roundNum, COORDINATOR}
			}
			startedElection = -100
		}
		if checkedLeader == roundNum-2 {
			for i := pid + 1; i < leader; i++ {
				comm[i] <- Message{pid, roundNum, ELECTION}
			}
			startedElection = roundNum
		}
		lastWitnessRound = roundNum
	}
}

// assumes messages from previous rounds have been read already
func getMessages(msgs chan Message, round int) []Message {
	var result []Message

	// check if there are messages to be read
	if len(msgs) > 0 {
		var m Message

		// read all messages belonging to the corresponding round
		for m = <-msgs; m.Round == round; m = <-msgs {
			result = append(result, m)
			if len(msgs) == 0 {
				break
			}
		}

		// insert back message from any other round
		if m.Round != round {
			msgs <- m
		}
	}
	return result
}
