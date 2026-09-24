package agent

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"sigs.k8s.io/yaml"

	"github.com/fduser123-coding/turgon/pkg/catalog"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/verifier"
)

// testdata/agentgateway-v1.5.0-config.schema.json is schema/config.json
// from agentgateway v1.5.0 (Apache-2.0), the configuration schema the
// gateway itself publishes.
func gatewaySchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	f, err := os.Open("testdata/agentgateway-" + GatewayVersion + "-config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	doc, err := jsonschema.UnmarshalJSON(f)
	if err != nil {
		t.Fatal(err)
	}
	c := jsonschema.NewCompiler()
	if err := c.AddResource("agentgateway.json", doc); err != nil {
		t.Fatal(err)
	}
	s, err := c.Compile("agentgateway.json")
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func compileExample(t *testing.T, name string) *compiler.RuntimeSpec {
	t.Helper()
	cat, err := catalog.Load("../../examples")
	if err != nil {
		t.Fatal(err)
	}
	obj, err := cat.Find(name)
	if err != nil {
		t.Fatal(err)
	}
	spec, _, err := compiler.Compile(cat, obj, verifier.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return spec
}

func gatewayOptions() GatewayOptions {
	return GatewayOptions{
		Issuer: "https://login.example.com", Audiences: []string{"turgon"}, JWKS: "https://login.example.com/jwks.json",
		RateLimit: RateLimit{Requests: 100, Per: time.Minute},
	}
}

func decode(t *testing.T, cfg []byte) any {
	t.Helper()
	js, err := yaml.YAMLToJSON(cfg)
	if err != nil {
		t.Fatal(err)
	}
	v, err := jsonschema.UnmarshalJSON(strings.NewReader(string(js)))
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestGatewayConfigMatchesAgentgatewaySchema(t *testing.T) {
	schema := gatewaySchema(t)
	routes := []GatewayRoute{
		{Name: "won-deals", Spec: compileExample(t, "salesforce-won-deals-to-erp"), Upstream: "http://127.0.0.1:8091/"},
		{Name: "shop", Spec: compileExample(t, "shop-orders-to-erp"), Upstream: "http://127.0.0.1:8090/", Writes: true},
	}
	cfg, err := GatewayConfig(routes, gatewayOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := schema.Validate(decode(t, cfg)); err != nil {
		t.Fatalf("not a valid agentgateway config: %v\n%s", err, cfg)
	}

	// The schema is strict enough to catch mistakes.
	var doc map[string]any
	if err := yaml.Unmarshal(cfg, &doc); err != nil {
		t.Fatal(err)
	}
	route := doc["binds"].([]any)[0].(map[string]any)["listeners"].([]any)[0].(map[string]any)["routes"].([]any)[0].(map[string]any)
	route["policies"].(map[string]any)["jwtAuth"].(map[string]any)["mode"] = "lenient"
	b, _ := json.Marshal(doc)
	bad, _ := jsonschema.UnmarshalJSON(strings.NewReader(string(b)))
	if schema.Validate(bad) == nil {
		t.Fatal("schema accepted an invalid JWT mode")
	}
}

func TestGatewayAuthenticatesAndAllowsToolsByRiskTier(t *testing.T) {
	shop := compileExample(t, "shop-orders-to-erp")
	cfg, err := GatewayConfig([]GatewayRoute{
		{Name: "shop", Spec: shop, Upstream: "http://127.0.0.1:8090/", Writes: true},
		{Name: "reads", Spec: shop, Upstream: "http://127.0.0.1:8091/"},
	}, gatewayOptions())
	if err != nil {
		t.Fatal(err)
	}
	s := string(cfg)
	for _, want := range []string{
		"mode: strict",
		"exact: /shop/mcp",
		"x-agent-id: jwt.azp",
		"x-on-behalf-of: jwt.sub",
		`x-agent-roles: jwt.roles.join(",")`,
		"fillInterval: 60s",
		`mcp.tool.name in ["create_sales_order"] && "integration-operator" in jwt.roles`,
	} {
		if !strings.Contains(strings.Join(strings.Fields(s), " "), want) {
			t.Errorf("config lacks %q:\n%s", want, s)
		}
	}
	// A spec served without --writes exposes no write tool through the gateway.
	var doc struct {
		Binds []struct {
			Listeners []struct {
				Routes []struct {
					Name     string
					Policies struct {
						McpAuthorization struct{ Rules []string } `json:"mcpAuthorization"`
					}
				}
			}
		}
	}
	if err := yaml.Unmarshal(cfg, &doc); err != nil {
		t.Fatal(err)
	}
	for _, r := range doc.Binds[0].Listeners[0].Routes {
		rules := strings.Join(r.Policies.McpAuthorization.Rules, "\n")
		if r.Name == "reads" && strings.Contains(rules, "create_sales_order") {
			t.Errorf("read-only route allows a write tool: %s", rules)
		}
	}
}

func TestGatewayConfigRejectsUnsafeInput(t *testing.T) {
	shop := compileExample(t, "shop-orders-to-erp")
	ok := []GatewayRoute{{Name: "shop", Spec: shop, Upstream: "http://127.0.0.1:8090/"}}
	cases := map[string]func() ([]byte, error){
		"no issuer": func() ([]byte, error) { o := gatewayOptions(); o.Issuer = ""; return GatewayConfig(ok, o) },
		"no routes": func() ([]byte, error) { return GatewayConfig(nil, gatewayOptions()) },
		"CEL in a claim name": func() ([]byte, error) {
			o := gatewayOptions()
			o.AgentClaim = `sub) || true || (jwt.sub`
			return GatewayConfig(ok, o)
		},
		"bad route name": func() ([]byte, error) {
			return GatewayConfig([]GatewayRoute{{Name: "Shop/../x", Spec: shop}}, gatewayOptions())
		},
		"duplicate route": func() ([]byte, error) { return GatewayConfig(append(ok, ok...), gatewayOptions()) },
	}
	for name, f := range cases {
		if _, err := f(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	o := gatewayOptions()
	o.UserClaim = "act.sub"
	cfg, err := GatewayConfig(ok, o)
	if err != nil || !strings.Contains(string(cfg), "x-on-behalf-of: jwt.act.sub") {
		t.Errorf("nested claim: %v", err)
	}
}
