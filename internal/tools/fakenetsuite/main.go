// Command fakenetsuite runs the test fake of NetSuite's REST web services
// for local demos, with token-based authentication, and prints each sales
// order as it is created or deleted.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector/rest"
	"github.com/fduser123-coding/turgon/pkg/connector/rest/netsuitetest"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:9900", "address to listen on")
	account := flag.String("account", "1234567_SB1", "account ID (the OAuth realm)")
	flag.Parse()
	creds := rest.OAuth1Credentials{ConsumerKey: "ck-demo", ConsumerSecret: "cs-demo", TokenID: "ti-demo", TokenSecret: "ts-demo"}
	ns := netsuitetest.NewAccount(*account, creds)
	// Internal IDs keep increasing across restarts, as in a real account.
	ns.StartAt(int(time.Now().Unix()%1_000_000)*10, time.Now)
	go func() { log.Fatal(http.ListenAndServe(*addr, ns)) }()
	fmt.Fprintf(os.Stderr, "fake NetSuite account %s at http://%s/services (consumer ck-demo/cs-demo, token ti-demo/ts-demo)\n", *account, *addr)
	fmt.Fprintln(os.Stderr, "customers: 1001 Lovelace GmbH, 1002 Hopper Industries; items: 101 (M-1, 10.00), 102 (M-2, 235.00)")
	seen := map[string]bool{}
	for range time.Tick(500 * time.Millisecond) {
		now := map[string]bool{}
		for _, id := range ns.OrderIDs() {
			now[id] = true
			if !seen[id] {
				o := ns.Order(id)
				fmt.Fprintf(os.Stderr, "sales order %s (%v) created: customer %v, total %v, external ID %v\n", id, o["tranId"], o["entity"], o["total"], o["externalId"])
			}
		}
		var gone []string
		for id := range seen {
			if !now[id] {
				gone = append(gone, id)
			}
		}
		sort.Strings(gone)
		for _, id := range gone {
			fmt.Fprintf(os.Stderr, "sales order %s deleted\n", id)
		}
		seen = now
	}
}
