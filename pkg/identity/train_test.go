package identity

import (
	"fmt"
	"math/rand"
	"testing"
)

// A deployment where the defaults are wrong: many buyers come through a
// marketplace that masks their addresses behind one relay domain, with no
// name. The default model takes a shared domain as strong evidence and
// scores different buyers 0.88 alike: stewards see them suggested as
// likely matches, and a recipe auto-matching above 0.8 would link them.
// Trained on the deployment's own links, it learns the domain says little.
func marketplace() []Record {
	rng := rand.New(rand.NewSource(7))
	word := func() string {
		b := make([]byte, 6+rng.Intn(5))
		for i := range b {
			b[i] = byte('a' + rng.Intn(26))
		}
		return string(b)
	}
	var rs []Record
	for i := 0; i < 300; i++ {
		master := fmt.Sprintf("C-%03d", i)
		w := word()
		company := w + ".example"
		name := w + " " + word() + " GmbH"
		// Direct buyers: company addresses and names, several people each.
		rs = append(rs,
			Record{master, Attributes{"email": "buyer@" + company, "domain": company, "name": normalizeName(name)}},
			Record{master, Attributes{"email": "orders@" + company, "domain": company, "name": normalizeName(name)}},
		)
		// Marketplace orders: a relay address per order, no name.
		for j := 0; j < 2; j++ {
			rs = append(rs, Record{master, Attributes{"email": fmt.Sprintf("r%03d-%d@relay.marketplace.example", i, j), "domain": "relay.marketplace.example"}})
		}
	}
	return rs
}

func TestTrainingLearnsWhatADeploymentsDataSays(t *testing.T) {
	m, rep := Train(marketplace(), TrainOptions{Threshold: 0.8})
	if rep.Matching == 0 || rep.NonMatching == 0 || rep.HeldOutMatching == 0 {
		t.Fatalf("report = %+v", rep)
	}
	// A shared domain is common among different customers here.
	if m.Levels[DomainSame].U <= 10*Default.Levels[DomainSame].U {
		t.Errorf("u(domain.same) = %v, default %v: not learned", m.Levels[DomainSame].U, Default.Levels[DomainSame].U)
	}
	// On held-out customers the default model links different ones; the
	// trained model does not, and still links most records that match.
	if rep.Default.FalseMatches == 0 {
		t.Fatalf("the default model made no false matches; the test data does not exercise it: %+v", rep.Default)
	}
	if rep.Trained.FalseMatches >= rep.Default.FalseMatches || rep.Trained.Precision <= rep.Default.Precision {
		t.Errorf("trained %+v is no better than default %+v", rep.Trained, rep.Default)
	}
	// Of each customer's six pairs only the two direct buyers (same name,
	// same company domain) can be told apart from other customers; relay
	// orders carry nothing that identifies the buyer. The trained model
	// must find all of those.
	if rep.Trained.Recall < 1.0/6-0.01 {
		t.Errorf("trained recall %.3f: it misses pairs that can be identified", rep.Trained.Recall)
	}
	t.Logf("held out: default %+v, trained %+v", rep.Default, rep.Trained)
}

// With little data the model stays close to the defaults.
func TestTrainingOnLittleDataKeepsTheDefaults(t *testing.T) {
	m, _ := Train([]Record{
		{"C-1", Attributes{"email": "a@x.example", "domain": "x.example"}},
		{"C-1", Attributes{"email": "b@x.example", "domain": "x.example"}},
		{"C-2", Attributes{"email": "c@y.example", "domain": "y.example"}},
	}, TrainOptions{Holdout: 0.01})
	for _, l := range Levels {
		d, got := Default.Levels[l], m.Levels[l]
		if got.M < d.M/2 || got.M > d.M*2+0.1 || got.U > d.U*50+0.1 {
			t.Errorf("%s: %+v strayed far from the default %+v on three records", l, got, d)
		}
	}
}

func TestTrainingIsDeterministic(t *testing.T) {
	a, _ := Train(marketplace(), TrainOptions{MaxNonMatching: 5000})
	b, _ := Train(marketplace(), TrainOptions{MaxNonMatching: 5000})
	for _, l := range Levels {
		if a.Levels[l] != b.Levels[l] {
			t.Fatalf("%s differs between runs", l)
		}
	}
}
