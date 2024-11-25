package client

func (c *Client) communicateWithServer(value uint64) uint64 {

	// req := server.PrepareRequest{ProposalNumber: 1}
	// rep := server.PrepareReply{}

	// majority := (len(c.Servers) / 2) + 1

	// voted := 1
	// var l sync.Mutex
	// cond := sync.NewCond(&l)

	// for i := range c.Servers {
	// 	go func() {
	// 		// put a lock here and if a majority respond
	// 		protocol.Invoke(*c.Servers[i], "Server.PrepareRequest", req, rep)
	// 		l.Lock()
	// 			rf.mu.Lock()

	// 			voted += 1
	// 			if reply.VoteGranted && rf.state == 1 {
	// 				voteCount += 1
	// 			}

	// 			rf.mu.Unlock()
	// 			l.Unlock()

	// 			cond.Broadcast()
	// 		resp += 1

	// 	}()
	// }

	// // condition variable here

	// for
}
