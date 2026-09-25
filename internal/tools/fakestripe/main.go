// Command fakestripe runs the test fake of the Stripe API for local demos.
// It accepts paid invoices on stdin, one "<customer> <amount>" per line
// (the amount in major units, e.g. 12.50), and prints each invoice's
// metadata when it changes.
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
	flag.Parse()
	st := stripetest.NewStripe(*key)
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
		if len(f) != 2 {
			fmt.Fprintln(os.Stderr, "usage: <customer> <amount>")
			continue
		}
		amount, err := strconv.ParseFloat(f[1], 64)
		if err != nil || amount < 0 {
			fmt.Fprintln(os.Stderr, "amount must be a positive number")
			continue
		}
		id := st.PayInvoice(f[0], int64(math.Round(amount*100)), "eur")
		mu.Lock()
		ids = append(ids, id)
		seen[id] = fmt.Sprint(st.Invoice(id)["metadata"])
		mu.Unlock()
		fmt.Fprintf(os.Stderr, "invoice %s paid by %s: %.2f EUR\n", id, f[0], amount)
	}
	select {}
}
