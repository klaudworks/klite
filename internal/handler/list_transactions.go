package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func HandleListTransactions(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.ListTransactionsRequest)
		resp := r.ResponseKind().(*kmsg.ListTransactionsResponse)

		snapshots := state.PIDManager().AllTransactions()

		stateFilter := make(map[string]bool)
		for _, s := range r.StateFilters {
			stateFilter[s] = true
		}

		pidFilter := make(map[int64]bool)
		for _, pid := range r.ProducerIDFilters {
			pidFilter[pid] = true
		}

		for _, snap := range snapshots {
			var txnState string
			if snap.TxnState == cluster.TxnOngoing {
				txnState = "Ongoing"
			} else {
				txnState = "Empty"
			}

			if len(stateFilter) > 0 && !stateFilter[txnState] {
				continue
			}

			if len(pidFilter) > 0 && !pidFilter[snap.ProducerID] {
				continue
			}

			ts := kmsg.NewListTransactionsResponseTransactionState()
			ts.TransactionalID = snap.TxnID
			ts.ProducerID = snap.ProducerID
			ts.TransactionState = txnState

			resp.TransactionStates = append(resp.TransactionStates, ts)
		}

		return resp, nil
	}
}
