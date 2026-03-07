package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func HandleDescribeTransactions(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.DescribeTransactionsRequest)
		resp := r.ResponseKind().(*kmsg.DescribeTransactionsResponse)

		for _, txnID := range r.TransactionalIDs {
			ts := kmsg.NewDescribeTransactionsResponseTransactionState()
			ts.TransactionalID = txnID

			snap, ok := state.PIDManager().GetProducerByTxnIDSnapshot(txnID)
			if !ok {
				ts.ErrorCode = kerr.TransactionalIDNotFound.Code
				resp.TransactionStates = append(resp.TransactionStates, ts)
				continue
			}

			ts.ProducerID = snap.ProducerID
			ts.ProducerEpoch = snap.Epoch
			ts.TimeoutMillis = snap.TxnTimeoutMs

			if snap.TxnState == cluster.TxnOngoing {
				ts.State = "Ongoing"
				ts.StartTimestamp = snap.TxnStartTime.UnixMilli()

				topicParts := make(map[string][]int32)
				for tp := range snap.TxnPartitions {
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
