// Command fakesf runs the test fake of the Salesforce API for local demos.
// It prints the credentials JSON a connection's secret should hold and
// accepts new won deals on stdin, one "<AccountId> <Amount>" per line.
// Each is also published as a change event on the fake Pub/Sub API
// (-pubsub-listen), for connections that subscribe to
// /data/OpportunityChangeEvent.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector/salesforce/sftest"
)

func main() {
	pubsubAddr := flag.String("pubsub-listen", "127.0.0.1:7443", "address of the fake Pub/Sub API (plaintext gRPC)")
	flag.Parse()
	sf := sftest.New()
	defer sf.Close()
	sf.PubSubAddr = *pubsubAddr
	fmt.Fprintf(os.Stderr, "fake Salesforce at %s, Pub/Sub API at %s\n", sf.URL, sf.PubSub())
	fmt.Println(sf.Credentials())
	n := 0
	in := bufio.NewScanner(os.Stdin)
	for in.Scan() {
		f := strings.Fields(in.Text())
		if len(f) != 2 {
			fmt.Fprintln(os.Stderr, "usage: <AccountId> <Amount>")
			continue
		}
		amount, _ := strconv.ParseFloat(f[1], 64)
		n++
		id := fmt.Sprintf("006%012dAAA", n)
		sf.Put("Opportunity", id, map[string]any{
			"StageName": "Closed Won", "AccountId": f[0], "Amount": amount,
			"CloseDate": time.Now().Format("2006-01-02"), "CurrencyIsoCode": "EUR",
			"OpportunityLineItems": map[string]any{"totalSize": 1, "done": true, "records": []any{
				map[string]any{"Quantity": 1, "Product2": map[string]any{"ProductCode": "M-1"}},
			}},
		}, time.Now())
		sf.PublishChange(sftest.Change{Type: "UPDATE", RecordIDs: []string{id}, Fields: map[string]any{"StageName": "Closed Won"}})
		fmt.Fprintf(os.Stderr, "won deal %s for %s: %.2f\n", id, f[0], amount)
	}
	for {
		opp := sf.Get("Opportunity", "006000000000001AAA")
		fmt.Fprintf(os.Stderr, "stdin closed; opportunity 1 ERP number: %v\n", opp["ERP_Order_Number__c"])
		time.Sleep(time.Hour)
	}
}
