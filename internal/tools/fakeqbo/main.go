// Command fakeqbo runs the test fake of the QuickBooks Online Accounting API
// and Intuit's token endpoint for local demos. It prints the first refresh
// token (the one a person's authorization would give) on stdout, and each
// sales receipt as it is created or voided on stderr. Each refresh rotates
// the refresh token, so only Turgon's token vault can keep using it.
// "delete <document number>" on stdin deletes a receipt, as a person in
// QuickBooks would; change data capture then reports it.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector/rest/qbotest"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:9870", "address to listen on")
	flag.Parse()
	q, seed := qbotest.NewQBO("qb-demo", "qb-demo-secret")
	q.StartAt(1000+int(time.Now().Unix()%10_000)*10, time.Now)
	go func() { log.Fatal(http.ListenAndServe(*addr, q)) }()
	fmt.Println(seed)
	fmt.Fprintf(os.Stderr, "fake QuickBooks Online at http://%s%s (client qb-demo / qb-demo-secret, tokens at http://%s%s)\n", *addr, qbotest.API, *addr, qbotest.TokenPath)
	fmt.Fprintln(os.Stderr, "customers: 1 Amy's Bird Sanctuary (ada@example.com), 2 Bill's Windsurf Shop; items: 11 Garden Supplies (M-1), 12 Pump (M-2)")
	go func() {
		in := bufio.NewScanner(os.Stdin)
		for in.Scan() {
			cmd, doc, _ := strings.Cut(strings.TrimSpace(in.Text()), " ")
			if cmd != "delete" || doc == "" {
				fmt.Fprintln(os.Stderr, "usage: delete <document number>")
				continue
			}
			r := q.ReceiptByDocNumber(doc)
			if r == nil || !q.Delete(fmt.Sprint(r["Id"])) {
				fmt.Fprintf(os.Stderr, "no sales receipt %s\n", doc)
				continue
			}
			fmt.Fprintf(os.Stderr, "sales receipt %s (id %v) deleted\n", doc, r["Id"])
		}
	}()
	seen := map[string]string{}
	refreshes := 0
	for range time.Tick(500 * time.Millisecond) {
		if q.Refreshes != refreshes {
			refreshes = q.Refreshes
			fmt.Fprintf(os.Stderr, "token refreshed (%d so far, %d spent tokens refused); the refresh token rotated\n", q.Refreshes, q.Spent)
		}
		for _, id := range q.ReceiptIDs() {
			r := q.Receipt(id)
			if r == nil { // deleted meanwhile
				continue
			}
			state := fmt.Sprint(r["SyncToken"])
			if seen[id] == state {
				continue
			}
			if seen[id] == "" {
				fmt.Fprintf(os.Stderr, "sales receipt %v (id %s) created: customer %v, total %v\n", r["DocNumber"], id, r["CustomerRef"].(map[string]any)["value"], r["TotalAmt"])
			} else {
				fmt.Fprintf(os.Stderr, "sales receipt %v (id %s) updated: total %v, note %v\n", r["DocNumber"], id, r["TotalAmt"], r["PrivateNote"])
			}
			seen[id] = state
		}
	}
}
