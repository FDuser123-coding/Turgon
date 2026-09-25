// Command fakeshop runs the test fake of the Shopify Admin REST API for
// local demos. It accepts new orders on stdin, one "<email> <amount>" per
// line, and prints each order's note when it changes.
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

	"github.com/fduser123-coding/turgon/pkg/connector/rest/shoptest"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:9300", "address to listen on")
	token := flag.String("token", "shpat_demo", "access token the store accepts")
	flag.Parse()
	shop := shoptest.NewShop(*token)
	go func() { log.Fatal(http.ListenAndServe(*addr, shop)) }()
	fmt.Fprintf(os.Stderr, "fake Shopify at http://%s/admin/api/%s (token %s)\n", *addr, shoptest.Version, *token)

	var mu sync.Mutex
	var ids []int64
	notes := map[int64]any{}
	go func() {
		for range time.Tick(time.Second) {
			mu.Lock()
			for _, id := range ids {
				if o := shop.Order(id); o != nil && fmt.Sprint(o["note"]) != fmt.Sprint(notes[id]) {
					notes[id] = o["note"]
					fmt.Fprintf(os.Stderr, "order %d note: %v\n", id, o["note"])
				}
			}
			mu.Unlock()
		}
	}()
	in := bufio.NewScanner(os.Stdin)
	for n := 1001; in.Scan(); n++ {
		f := strings.Fields(in.Text())
		if len(f) != 2 {
			fmt.Fprintln(os.Stderr, "usage: <email> <amount>")
			continue
		}
		if _, err := strconv.ParseFloat(f[1], 64); err != nil {
			fmt.Fprintln(os.Stderr, "amount must be a number")
			continue
		}
		id := shop.AddOrder(map[string]any{
			"name": fmt.Sprintf("#%d", n), "email": f[0], "created_at": time.Now().Format(time.RFC3339),
			"subtotal_price": f[1], "currency": "EUR", "line_items": []any{map[string]any{"sku": "M-1", "quantity": 1}},
		})
		mu.Lock()
		ids = append(ids, id)
		notes[id] = nil
		mu.Unlock()
		fmt.Fprintf(os.Stderr, "order %d (#%d) for %s: %s EUR\n", id, n, f[0], f[1])
	}
	select {}
}
