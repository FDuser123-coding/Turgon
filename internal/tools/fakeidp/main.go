// Command fakeidp runs the test fake of an OpenID Connect provider for
// local demos of the console's sign-in. Its sign-in page lists the
// accounts given with -account, each "email:group,group".
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/fduser123-coding/turgon/internal/oidctest"
)

type accounts []oidctest.Account

func (a *accounts) String() string { return fmt.Sprint(*a) }

func (a *accounts) Set(v string) error {
	email, groups, _ := strings.Cut(v, ":")
	if !strings.Contains(email, "@") {
		return fmt.Errorf("account %q: want email:group,group", v)
	}
	acc := oidctest.Account{Subject: fmt.Sprintf("user-%d", len(*a)+1), Email: email}
	if groups != "" {
		acc.Groups = strings.Split(groups, ",")
	}
	*a = append(*a, acc)
	return nil
}

func main() {
	addr := flag.String("listen", "127.0.0.1:9800", "address to listen on")
	client := flag.String("client-id", "turgon-console", "the console's client ID")
	secret := flag.String("client-secret", "demo-secret", "the console's client secret")
	redirect := flag.String("redirect", "http://127.0.0.1:8095/auth/callback", "the console's callback URL")
	var accs accounts
	flag.Var(&accs, "account", "an account to offer, email:group,group (repeatable)")
	flag.Parse()
	if len(accs) == 0 {
		_ = accs.Set("olga@example.com:turgon-approvers,turgon-stewards,turgon-operators")
		_ = accs.Set("vera@example.com:turgon-viewers")
	}
	p := oidctest.NewProvider(*client, *secret, accs...)
	p.Issuer = "http://" + *addr
	p.RedirectURIs = []string{*redirect}
	p.Current = -1 // show the sign-in page
	fmt.Fprintf(os.Stderr, "fake identity provider at %s (client %s, secret %s)\n", p.Issuer, *client, *secret)
	for _, a := range accs {
		fmt.Fprintf(os.Stderr, "  %s %v\n", a.Email, a.Groups)
	}
	log.Fatal(http.ListenAndServe(*addr, p))
}
