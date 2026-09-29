// Command fakedynamics runs the test fake of the Dataverse Web API
// (Dynamics 365 Sales) for local demos, with its own OAuth 2.0 token
// endpoint, and prints each sales order as it is created or deleted.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector/rest/dataversetest"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:9700", "address to listen on")
	id := flag.String("client-id", "turgon-app", "app registration's client ID")
	secret := flag.String("client-secret", "demo", "its client secret")
	flag.Parse()
	dv := dataversetest.NewDataverse(*id, *secret)
	go func() { log.Fatal(http.ListenAndServe(*addr, dv)) }()
	fmt.Fprintf(os.Stderr, "fake Dataverse at http://%s%s, tokens at http://%s/oauth2/v2.0/token (client %s)\n", *addr, dataversetest.API, *addr, *id)
	fmt.Fprintf(os.Stderr, "accounts: %s Lovelace GmbH, %s Hopper Industries\n", dataversetest.LovelaceGmbH, dataversetest.HopperInc)
	seen := map[string]bool{}
	for range time.Tick(500 * time.Millisecond) {
		now := map[string]bool{}
		for _, id := range dv.OrderIDs() {
			now[id] = true
			if !seen[id] {
				o := dv.Order(id)
				fmt.Fprintf(os.Stderr, "sales order %s created: %v for account %v, total %v, key %v\n", id, o["name"], o["_customerid_value"], o["totalamount"], o["turgon_externalref"])
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
