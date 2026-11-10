// Command fakeexact runs the test fake of the Exact Online REST API and its
// OAuth 2.0 token endpoint for local demos. It prints the first refresh
// token (the one a person's authorization would give) on stdout, and each
// sales order as it is created or deleted on stderr. Each refresh rotates
// the refresh token, so only Turgon's token vault can keep using it.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector/rest/exacttest"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:9890", "address to listen on")
	flag.Parse()
	x, seed := exacttest.NewExact("exact-demo", "exact-demo-secret")
	x.StartAt(1+int(time.Now().Unix()%1000)*10, time.Now)
	go func() { log.Fatal(http.ListenAndServe(*addr, x)) }()
	fmt.Println(seed)
	fmt.Fprintf(os.Stderr, "fake Exact Online at http://%s%s (client exact-demo / exact-demo-secret, tokens at http://%s%s)\n", *addr, exacttest.API, *addr, exacttest.TokenPath)
	fmt.Fprintf(os.Stderr, "accounts: %s Lovelace GmbH (ada@example.com), %s Hopper Industries (blocked); items: M-1 Desk (100.00), M-2 Chair (235.00)\n", exacttest.Lovelace, exacttest.Hopper)
	seen := map[string]bool{}
	refreshes := 0
	for range time.Tick(500 * time.Millisecond) {
		if n, spent := x.TokenCounts(); n != refreshes {
			refreshes = n
			fmt.Fprintf(os.Stderr, "token refreshed (%d so far, %d spent tokens refused); the refresh token rotated\n", n, spent)
		}
		now := map[string]bool{}
		for _, id := range x.OrderIDs() {
			now[id] = true
			if seen[id] {
				continue
			}
			o := x.Order(id)
			fmt.Fprintf(os.Stderr, "sales order %v (%v) for %v: %v %v\n", o["OrderNumber"], o["YourRef"], o["OrderedByName"], o["AmountDC"], o["Currency"])
		}
		for id := range seen {
			if !now[id] {
				fmt.Fprintf(os.Stderr, "sales order %s deleted\n", id)
			}
		}
		seen = now
	}
}
