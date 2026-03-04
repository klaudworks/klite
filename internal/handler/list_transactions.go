package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// HandleListTransactions returns the ListTransactions handler (API key 66).
// Supports v0-v1.
func HandleListTransactions(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.ListTransactionsRequest)
		resp := r.ResponseKind().(*kmsg.ListTransactionsResponse)

		allTxns := state.PIDManager().AllTransactions()

		// Build state filter set
		stateFilter := make(map[string]bool)
		for _, s := range r.StateFilters {
			stateFilter[s] = true
		}

		// Build producer ID filter set
		pidFilter := make(map[int64]bool)
		for _, pid := range r.ProducerIDFilters {
			pidFilter[pid] = true
		}

		for _, ps := range allTxns {
			var txnState string
			if ps.TxnState == cluster.TxnOngoing {
				txnState = "Ongoing"
			} else {
				txnState = "Empty"
			}

			// Apply state filter
			if len(stateFilter) > 0 && !stateFilter[txnState] {
				continue
			}

			// Apply producer ID filter
			if len(pidFilter) > 0 && !pidFilter[ps.ProducerID] {
				continue
			}

			ts := kmsg.NewListTransactionsResponseTransactionState()
			ts.TransactionalID = ps.TxnID
			ts.ProducerID = ps.ProducerID
			ts.TransactionState = txnState

			resp.TransactionStates = append(resp.TransactionStates, ts)
		}

		return resp, nil
	}
}
