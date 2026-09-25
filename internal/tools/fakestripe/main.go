// Command fakestripe runs the test fake of the Stripe API for local demos.
// It accepts paid invoices on stdin, one "<customer> <amount>" per line
// (the amount in major units, e.g. 12.50; add "nohook" to skip the
// webhook delivery), and prints each invoice's metadata when it changes.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector/rest/stripetest"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:9400", "address to listen on")
	key := flag.String("key", "rk_test_demo", "secret key the account accepts")
	hookURL := flag.String("webhook-url", "", "also post each event to this webhook endpoint, e.g. http://127.0.0.1:8082/webhooks/stripe-billing/Invoice.Paid")
	hookSecret := flag.String("webhook-secret", "whsec_demo", "the webhook endpoint's signing secret")
	flag.Parse()
	st := stripetest.NewStripe(*key)
	// IDs and event times keep increasing across restarts, as in a real
	// account, so a demo never re-reads what an earlier demo handled.
	// Numbered from milliseconds, a restart never reaches the numbers an
	// earlier run used, even after thousands of invoices.
	st.StartAt(int(time.Now().UnixMilli()%100_000_000_000), time.Now())
	if *hookURL != "" {
		st.SendWebhooks(*hookURL, *hookSecret)
	}
	go func() { log.Fatal(http.ListenAndServe(*addr, st)) }()
	fmt.Fprintf(os.Stderr, "fake Stripe at http://%s (key %s)\n", *addr, *key)

	var mu sync.Mutex
	seen := map[string]string{}
	var ids []string
	go func() {
		for range time.Tick(time.Second) {
			mu.Lock()
			for _, id := range ids {
				md := fmt.Sprint(st.Invoice(id)["metadata"])
				if md != seen[id] {
					seen[id] = md
					fmt.Fprintf(os.Stderr, "invoice %s metadata: %s\n", id, md)
				}
			}
			mu.Unlock()
		}
	}()
	in := bufio.NewScanner(os.Stdin)
	for in.Scan() {
		f := strings.Fields(in.Text())
		drop := len(f) == 3 && f[2] == "nohook"
		if drop {
			f = f[:2]
		}
		if len(f) != 2 {
			fmt.Fprintln(os.Stderr, "usage: <customer> <amount> [nohook]")
			continue
		}
		amount, err := strconv.ParseFloat(f[1], 64)
		if err != nil || amount < 0 {
			fmt.Fprintln(os.Stderr, "amount must be a positive number")
			continue
		}
		st.DropWebhooks = drop // as if the delivery failed until Stripe gave up
		id := st.PayInvoice(f[0], int64(math.Round(amount*100)), "eur")
		mu.Lock()
		ids = append(ids, id)
		seen[id] = fmt.Sprint(st.Invoice(id)["metadata"])
		mu.Unlock()
		fmt.Fprintf(os.Stderr, "invoice %s paid by %s: %.2f EUR\n", id, f[0], amount)
		switch {
		case drop:
			fmt.Fprintln(os.Stderr, "invoice.paid webhook: not delivered")
		case *hookURL != "":
			fmt.Fprintf(os.Stderr, "invoice.paid webhook: HTTP %d\n", st.Deliveries[len(st.Deliveries)-1])
		}
	}
	select {}
}
