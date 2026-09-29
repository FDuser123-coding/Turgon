// Command fakesap runs the test fake of SAP S/4HANA's sales order and
// business partner OData APIs for local demos, and prints each sales order
// as it is created, changed or deleted. With -mesh it also runs a fake SAP
// Event Mesh instance, to which the system publishes its sales order events
// (queue acme/s4/turgon/salesorders). On stdin, "change <order>" touches an
// order and "order <customer> <material> <quantity>" enters one, as a
// person in SAP would.
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
	addr := flag.String("listen", "127.0.0.1:9600", "address to listen on")
	user := flag.String("user", "TURGON_COMM", "communication user")
	password := flag.String("password", "demo", "its password")
	client := flag.String("client", "", "SAP client the system requires (sap-client), e.g. 100")
	meshAddr := flag.String("mesh", "", "also run a fake Event Mesh instance at this address, e.g. 127.0.0.1:9601")
	meshClient := flag.String("mesh-client", "sb-turgon!b1|xbem-service-broker!b2", "its OAuth client ID")
	meshSecret := flag.String("mesh-secret", "demo", "its client secret")
	flag.Parse()
	s4 := saptest.New(*user, *password, *client)
	if *meshAddr != "" {
		const queue = "acme/s4/turgon/salesorders"
		mesh := saptest.NewMesh(*meshClient, *meshSecret)
		mesh.CreateQueue(queue)
		s4.Events = mesh.Publisher(queue, "/default/sap.s4.beh/100")
		go func() { log.Fatal(http.ListenAndServe(*meshAddr, mesh)) }()
		fmt.Fprintf(os.Stderr, "fake Event Mesh at http://%s (token %s, queue %s)\n", *meshAddr, saptest.TokenPath, queue)
	}
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
		if len(f) == 4 && f[0] == "order" {
			if _, err := s4.CreateOrder(f[1], f[2], f[3]); err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
			continue
		}
		fmt.Fprintln(os.Stderr, "usage: change <order> | order <customer> <material> <quantity>")
	}
	select {}
}
