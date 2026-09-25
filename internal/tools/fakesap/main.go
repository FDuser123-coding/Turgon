// Command fakesap runs the test fake of SAP S/4HANA's sales order and
// business partner OData APIs for local demos, and prints each sales order
// as it is created, changed or deleted. On stdin, "change <order>" touches
// an order as a person in SAP would.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector/sap/saptest"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:9500", "address to listen on")
	user := flag.String("user", "TURGON_COMM", "communication user")
	password := flag.String("password", "demo", "its password")
	client := flag.String("client", "", "SAP client the system requires (sap-client), e.g. 100")
	flag.Parse()
	s4 := saptest.New(*user, *password, *client)
	// Order numbers keep increasing across restarts, as in a real system.
	s4.StartAt(int(time.Now().Unix()%10_000_000)*10, time.Now)
	go func() { log.Fatal(http.ListenAndServe(*addr, s4)) }()
	fmt.Fprintf(os.Stderr, "fake S/4HANA at http://%s%s (user %s)\n", *addr, saptest.Prefix, *user)

	go func() {
		seen := map[string]string{}
		for range time.Tick(500 * time.Millisecond) {
			now := map[string]bool{}
			for _, id := range s4.Orders() {
				now[id] = true
				o := s4.Order(id)
				b, _ := json.Marshal(map[string]any{"soldTo": o["SoldToParty"], "reference": o["PurchaseOrderByCustomer"],
					"net": o["TotalNetAmount"], "currency": o["TransactionCurrency"], "items": o["to_Item"]})
				if seen[id] != string(b) {
					verb := "created"
					if seen[id] != "" {
						verb = "changed"
					}
					seen[id] = string(b)
					fmt.Fprintf(os.Stderr, "sales order %s %s: %s\n", id, verb, b)
				}
			}
			for id := range seen {
				if !now[id] {
					delete(seen, id)
					fmt.Fprintf(os.Stderr, "sales order %s deleted\n", id)
				}
			}
		}
	}()
	in := bufio.NewScanner(os.Stdin)
	for in.Scan() {
		f := strings.Fields(in.Text())
		if len(f) == 2 && f[0] == "change" {
			s4.Change(f[1], "SalesOrderType", "OR")
			continue
		}
		fmt.Fprintln(os.Stderr, "usage: change <order>")
	}
	select {}
}
