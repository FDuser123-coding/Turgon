// Command fakeb1 runs the test fake of SAP Business One's Service Layer for
// local demos, and prints each sales order as it is created or cancelled.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector/rest/b1test"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:9950", "address to listen on")
	flag.Parse()
	b1 := b1test.NewB1("SBODEMOUS", "turgon", "demo")
	// Document numbers keep increasing across restarts, as in a real company.
	b1.StartAt(int(time.Now().Unix()%100_000)*10, time.Now)
	go func() { log.Fatal(http.ListenAndServe(*addr, b1)) }()
	fmt.Fprintf(os.Stderr, "fake SAP Business One Service Layer at http://%s%s (company SBODEMOUS, user turgon / demo)\n", *addr, b1test.Base)
	fmt.Fprintln(os.Stderr, "customers: C20000 Maxi-Teq (ada@example.com), C23900 Norm Thompson, C99999 (frozen); items: A00001 (400.00), A00002 (182.00)")
	seen := map[int]string{}
	for range time.Tick(500 * time.Millisecond) {
		for _, e := range b1.OrderEntries() {
			o := b1.Order(e)
			state := fmt.Sprint(o["DocumentStatus"]) + "/" + fmt.Sprint(o["Cancelled"])
			if seen[e] == state {
				continue
			}
			if seen[e] == "" {
				fmt.Fprintf(os.Stderr, "sales order %v created: customer %v, total %v, NumAtCard %v, cancelled %v\n",
					o["DocNum"], o["CardCode"], o["DocTotal"], o["NumAtCard"], o["Cancelled"])
			} else {
				fmt.Fprintf(os.Stderr, "sales order %v is now %v, cancelled %v\n", o["DocNum"], o["DocumentStatus"], o["Cancelled"])
			}
			seen[e] = state
		}
	}
}
