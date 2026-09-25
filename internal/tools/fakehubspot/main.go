// Command fakehubspot runs the test fake of the HubSpot CRM API for local
// demos. It reads commands on stdin, one per line:
//
//	won <email> <amount>    a deal is created already won
//	open <email> <amount>   a deal is created in an open stage
//	win <deal ID>           an open deal is won
//
// and prints each deal's ERP order number when it changes.
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
	"sync"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector/rest/hubspottest"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:9500", "address to listen on")
	token := flag.String("token", "pat-demo", "private app access token the account accepts")
	flag.Parse()
	hs := hubspottest.NewHubSpot(*token)
	// Deal IDs and change times keep increasing across restarts, as in a
	// real account, so a demo never re-reads a deal an earlier demo handled.
	hs.StartAt(time.Now().Unix()*10, time.Now())
	go func() { log.Fatal(http.ListenAndServe(*addr, hs)) }()
	fmt.Fprintf(os.Stderr, "fake HubSpot at http://%s (token %s)\n", *addr, *token)

	var mu sync.Mutex
	var ids []string
	seen := map[string]string{}
	go func() {
		for range time.Tick(time.Second) {
			mu.Lock()
			for _, id := range ids {
				if n := hs.Deal(id)["erp_order_number"]; n != seen[id] {
					seen[id] = n
					fmt.Fprintf(os.Stderr, "deal %s erp_order_number: %q\n", id, n)
				}
			}
			mu.Unlock()
		}
	}()
	in := bufio.NewScanner(os.Stdin)
	for in.Scan() {
		f := strings.Fields(in.Text())
		switch {
		case len(f) == 3 && (f[0] == "won" || f[0] == "open"):
			if _, err := strconv.ParseFloat(f[2], 64); err != nil {
				fmt.Fprintln(os.Stderr, "amount must be a number")
				continue
			}
			stage := "closedwon"
			if f[0] == "open" {
				stage = "contractsent"
			}
			id := hs.AddDeal(map[string]string{
				"dealname": "Deal for " + f[1], "customer_email": f[1], "amount": f[2], "deal_currency_code": "EUR",
				"dealstage": stage, "closedate": time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
			})
			mu.Lock()
			ids = append(ids, id)
			mu.Unlock()
			fmt.Fprintf(os.Stderr, "deal %s (%s) for %s: %s EUR\n", id, stage, f[1], f[2])
		case len(f) == 2 && f[0] == "win":
			if hs.Deal(f[1]) == nil {
				fmt.Fprintf(os.Stderr, "no deal %s\n", f[1])
				continue
			}
			hs.SetStage(f[1], "closedwon")
			fmt.Fprintf(os.Stderr, "deal %s won\n", f[1])
		default:
			fmt.Fprintln(os.Stderr, "usage: won <email> <amount> | open <email> <amount> | win <deal ID>")
		}
	}
	select {}
}
