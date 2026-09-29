package rest

import (
	"crypto/sha1"
	"net/url"
	"strings"
	"testing"
	"time"
)

// The signature matches a published OAuth 1.0a example (Twitter's
// "Creating a signature", HMAC-SHA1), so the base string, parameter
// normalization and encoding are right; NetSuite uses the same with SHA-256.
func TestOAuth1SignatureMatchesPublishedExample(t *testing.T) {
	u, _ := url.Parse("https://api.twitter.com/1.1/statuses/update.json?include_entities=true&status=" +
		url.QueryEscape("Hello Ladies + Gentlemen, a signed OAuth request!"))
	h := signOAuth1(sha1.New, "HMAC-SHA1", "POST", u, "", OAuth1Credentials{
		ConsumerKey: "xvz1evFS4wEEPTGEFPHBog", ConsumerSecret: "kAcSOqF21Fu85e7zjz7ZN2U4ZRhfV3WpwPAoE3Z7kBw",
		TokenID: "370773112-GmHxMAgYyLbNEtIKZeRNFsMKPR9EyMZeS9weJAEb", TokenSecret: "LswwdoUaIvS8ltyTt5jkRh4J50vUPVVHtR2YPi5kE",
	}, time.Unix(1318622958, 0), "kYjzVBB8Y0ZFabxSWbWovY3uYSQ2pTgmZeNu2VS4cg")
	if !strings.Contains(h, `oauth_signature="hCtSmYh%2BiHYCEqBWrE7C7hYmtUk%3D"`) {
		t.Fatalf("header = %s", h)
	}
}

func TestOAuth1Header(t *testing.T) {
	u, _ := url.Parse("https://1234567-sb1.suitetalk.api.netsuite.com/services/rest/record/v1/salesOrder")
	h := SignOAuth1("POST", u, "1234567_SB1", OAuth1Credentials{ConsumerKey: "ck", ConsumerSecret: "cs", TokenID: "ti", TokenSecret: "ts"}, time.Unix(1790000000, 0), "n1")
	for _, want := range []string{`OAuth realm="1234567_SB1"`, `oauth_signature_method="HMAC-SHA256"`, `oauth_token="ti"`, `oauth_consumer_key="ck"`, `oauth_timestamp="1790000000"`, `oauth_nonce="n1"`} {
		if !strings.Contains(h, want) {
			t.Errorf("header lacks %s: %s", want, h)
		}
	}
}
