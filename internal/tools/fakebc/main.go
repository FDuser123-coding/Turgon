// Command fakebc runs the test fake of Dynamics 365 Business Central's API
// (v2.0) and Entra ID's token endpoint for local demos, and prints each
// sales order as it is created or deleted.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector/rest/bctest"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:9800", "address to listen on")
	flag.Parse()
	bc := bctest.NewBC("turgon-app", "demo")
	// Order numbers keep increasing across restarts, as in a real company.
	bc.StartAt(101001+int(time.Now().Unix()%100_000)*10, time.Now)
	go func() { log.Fatal(http.ListenAndServe(*addr, bc)) }()
	fmt.Fprintf(os.Stderr, "fake Business Central at http://%s%s (client turgon-app / demo, token at http://%s%s)\n", *addr, bctest.API, *addr, bctest.TokenPath)
	fmt.Fprintln(os.Stderr, "customers: 10000 Adatum Corporation, 20000 Trey Research, 40000 Alpine Ski House (blocked); items: 1896-S (1000.80), 1900-S (192.80)")
	seen := map[string]bool{}
	for range time.Tick(500 * time.Millisecond) {
		now := map[string]bool{}
		for _, id := range bc.OrderIDs() {
			now[id] = true
			if !seen[id] {
				o := bc.Order(id)
				fmt.Fprintf(os.Stderr, "sales order %v (%s) created: customer %v, total %v, external document %v\n",
					o["number"], id, o["customerNumber"], o["totalAmountExcludingTax"], o["externalDocumentNumber"])
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
