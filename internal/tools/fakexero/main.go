// Command fakexero runs the test fake of the Xero Accounting API and Xero's
// identity token endpoint for local demos. It prints the first refresh token
// (the one a person's authorization would give) on stdout, and each invoice
// as it is created or voided on stderr. Each refresh rotates the refresh
// token, so only Turgon's token vault can keep using it.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector/rest/xerotest"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:9880", "address to listen on")
	flag.Parse()
	q, seed := xerotest.NewXero("xero-demo", "xero-demo-secret")
	q.StartAt(1+int(time.Now().Unix()%1000)*10, time.Now)
	go func() { log.Fatal(http.ListenAndServe(*addr, q)) }()
	fmt.Println(seed)
	fmt.Fprintf(os.Stderr, "fake Xero at http://%s%s (client xero-demo / xero-demo-secret, tenant %s, tokens at http://%s%s)\n", *addr, xerotest.API, xerotest.Tenant, *addr, xerotest.TokenPath)
	fmt.Fprintf(os.Stderr, "contacts: %s Lovelace GmbH (ada@example.com), %s Hopper Industries; items: M-1 Desk (100.00), M-2 Chair (235.00)\n", xerotest.Lovelace, xerotest.Hopper)
	seen := map[string]string{}
	refreshes := 0
	for range time.Tick(500 * time.Millisecond) {
		if q.Refreshes != refreshes {
			refreshes = q.Refreshes
			fmt.Fprintf(os.Stderr, "token refreshed (%d so far, %d spent tokens refused); the refresh token rotated\n", q.Refreshes, q.Spent)
		}
		for _, id := range q.InvoiceIDs() {
			inv := q.Invoice(id)
			state := fmt.Sprint(inv["Status"])
			if seen[id] == state {
				continue
			}
			fmt.Fprintf(os.Stderr, "invoice %v (%s) for %v: total %v, %s\n", inv["InvoiceNumber"], inv["Reference"], inv["Contact"].(map[string]any)["Name"], inv["Total"], state)
			seen[id] = state
		}
	}
}
