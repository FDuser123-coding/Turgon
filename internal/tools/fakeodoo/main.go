// Command fakeodoo runs the test fake of Odoo's external JSON-2 API for
// local demos, and prints each quotation as it is created or cancelled.
// "require <model> <field>" on stdin makes a field required, as a module
// or Studio would, so the next discovery reports it. "cancel <id>" cancels
// a sales order, as a person would in Odoo.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector/odoo/odootest"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:9850", "address to listen on")
	key := flag.String("api-key", "odoo-demo", "the API key requests must carry")
	flag.Parse()
	od := odootest.NewOdoo(*key)
	// Quotation numbers keep increasing across restarts, as in a real database.
	od.StartAt(int(time.Now().Unix()%10_000)*10, time.Now)
	go func() { log.Fatal(http.ListenAndServe(*addr, od)) }()
	fmt.Fprintf(os.Stderr, "fake Odoo at http://%s (JSON-2 API, API key %s)\n", *addr, *key)
	fmt.Fprintln(os.Stderr, "customers: 7 Lovelace GmbH (ada@example.com), 8 Hopper Industries; products: 11 [M-1] Desk (100.00), 12 [M-2] Chair (235.00)")
	go func() {
		in := bufio.NewScanner(os.Stdin)
		for in.Scan() {
			f := strings.Fields(in.Text())
			if len(f) == 3 && f[0] == "require" {
				od.SetRequired(f[1], f[2])
				fmt.Fprintf(os.Stderr, "%s.%s is now required\n", f[1], f[2])
				continue
			}
			if len(f) == 2 && f[0] == "cancel" {
				if id, err := strconv.ParseInt(f[1], 10, 64); err == nil && od.Order(id) != nil {
					od.SetState(id, "cancel")
					continue
				}
				fmt.Fprintf(os.Stderr, "no sales order %s\n", f[1])
				continue
			}
			fmt.Fprintln(os.Stderr, "usage: require <model> <field> | cancel <order id>")
		}
	}()
	seen := map[int64]string{}
	for range time.Tick(500 * time.Millisecond) {
		for _, id := range od.OrderIDs() {
			o := od.Order(id)
			state, _ := o["state"].(string)
			if seen[id] == state {
				continue
			}
			if seen[id] == "" {
				fmt.Fprintf(os.Stderr, "quotation %v (id %d) created: customer %v, untaxed %v, customer reference %v, state %s\n",
					o["name"], id, o["partner_id"], o["amount_untaxed"], o["client_order_ref"], state)
			} else {
				fmt.Fprintf(os.Stderr, "quotation %v (id %d) is now %s\n", o["name"], id, state)
			}
			seen[id] = state
		}
	}
}
