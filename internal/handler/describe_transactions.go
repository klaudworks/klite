package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// HandleDescribeTransactions returns the DescribeTransactions handler (API key 65).
// Supports v0.
func HandleDescribeTransactions(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.DescribeTransactionsRequest)
		resp := r.ResponseKind().(*kmsg.DescribeTransactionsResponse)

		for _, txnID := range r.TransactionalIDs {
			ts := kmsg.NewDescribeTransactionsResponseTransactionState()
			ts.TransactionalID = txnID

			ps := state.PIDManager().GetProducerByTxnID(txnID)
			if ps == nil {
				ts.ErrorCode = 79 // TRANSACTIONAL_ID_NOT_FOUND
				resp.TransactionStates = append(resp.TransactionStates, ts)
				continue
			}

			ts.ProducerID = ps.ProducerID
			ts.ProducerEpoch = ps.Epoch
			ts.TimeoutMillis = ps.TxnTimeoutMs

			if ps.TxnState == cluster.TxnOngoing {
				ts.State = "Ongoing"
				ts.StartTimestamp = ps.TxnStartTime.UnixMilli()

				// Build topic-partition list
				topicParts := make(map[string][]int32)
				for tp := range ps.TxnPartitions {
					topicParts[tp.Topic] = append(topicParts[tp.Topic], tp.Partition)
				}
				for topic, parts := range topicParts {
					t := kmsg.NewDescribeTransactionsResponseTransactionStateTopic()
					t.Topic = topic
					t.Partitions = parts
					ts.Topics = append(ts.Topics, t)
				}
			} else {
				ts.State = "Empty"
				ts.StartTimestamp = -1
			}

			resp.TransactionStates = append(resp.TransactionStates, ts)
		}

		return resp, nil
	}
}
