package identity

import (
	"hash/fnv"
	"math/rand"
	"sort"
	"strings"
)

// Record is a source record linked to its master record: training data.
type Record struct {
	Master     string
	Attributes Attributes
}

// TrainOptions tune training.
type TrainOptions struct {
	// Strength is how many pairs' worth of weight the default model keeps
	// for each estimate, so a level seen a few times cannot swing to an
	// extreme. Default 20.
	Strength float64
	// MaxNonMatching bounds the sampled pairs of different masters.
	// Default 200,000.
	MaxNonMatching int
	// Holdout is the share of masters kept out of training to evaluate
	// on. Default 0.3.
	Holdout float64
	// Threshold is the auto-match threshold evaluated. Default 0.95.
	Threshold float64
}

func (o TrainOptions) withDefaults() TrainOptions {
	if o.Strength <= 0 {
		o.Strength = 20
	}
	if o.MaxNonMatching <= 0 {
		o.MaxNonMatching = 200_000
	}
	if o.Holdout <= 0 || o.Holdout >= 1 {
		o.Holdout = 0.3
	}
	if o.Threshold <= 0 {
		o.Threshold = 0.95
	}
	return o
}

// LevelReport compares a level's trained and default probabilities.
type LevelReport struct {
	Level    string        `json:"level"`
	Default  Probabilities `json:"default"`
	Trained  Probabilities `json:"trained"`
	Matching int           `json:"matching"`    // matching pairs at this level
	NonMatch int           `json:"nonMatching"` // non-matching pairs at this level
}

// Quality is how a model separates held-out pairs at a threshold.
type Quality struct {
	Precision float64 `json:"precision"`
	Recall    float64 `json:"recall"`
	// FalseMatches are different entities scored at or above the threshold:
	// records that would be linked to the wrong master automatically.
	FalseMatches int `json:"falseMatches"`
}

// TrainReport says what training saw and how the model does on held-out
// masters, next to the default model.
type TrainReport struct {
	Records, Masters      int
	Matching, NonMatching int // training pairs
	Levels                []LevelReport
	Threshold             float64
	HeldOutMatching       int
	HeldOutNonMatching    int
	Default, Trained      Quality
}

type pair struct {
	a, b  Attributes
	match bool
}

// Train estimates each level's m and u from linked records: pairs of
// records linked to the same master are matches, sampled pairs of
// different masters are not. Estimates are blended with the default model
// by Strength. Masters are split by a hash of their ID into training and
// held-out sets, and both models are evaluated on the held-out pairs.
func Train(records []Record, opts TrainOptions) (Model, TrainReport) {
	o := opts.withDefaults()
	rep := TrainReport{Records: len(records), Threshold: o.Threshold}
	var train, held []Record
	masters := map[string]bool{}
	for _, r := range records {
		masters[r.Master] = true
		if heldOut(r.Master, o.Holdout) {
			held = append(held, r)
		} else {
			train = append(train, r)
		}
	}
	rep.Masters = len(masters)

	trainPairs := pairs(train, o.MaxNonMatching, 1)
	counts := map[string][2]int{} // level -> [matching, non-matching]
	groups := map[string][2]int{} // comparison kind -> pairs compared
	for _, p := range trainPairs {
		idx := 1
		if p.match {
			idx = 0
			rep.Matching++
		} else {
			rep.NonMatching++
		}
		seen := map[string]bool{}
		for _, oc := range compare(p.a, p.b) {
			c := counts[oc.level]
			c[idx]++
			counts[oc.level] = c
			g := group(oc.level)
			if !seen[g] {
				seen[g] = true
				n := groups[g]
				n[idx]++
				groups[g] = n
			}
		}
	}
	m := Model{Prior: Default.Prior, Levels: map[string]Probabilities{}}
	for _, level := range Levels {
		d := Default.Levels[level]
		c, g := counts[level], groups[group(level)]
		t := Probabilities{
			M: clamp((float64(c[0]) + o.Strength*d.M) / (float64(g[0]) + o.Strength)),
			U: clamp((float64(c[1]) + o.Strength*d.U) / (float64(g[1]) + o.Strength)),
		}
		m.Levels[level] = t
		rep.Levels = append(rep.Levels, LevelReport{Level: level, Default: d, Trained: t, Matching: c[0], NonMatch: c[1]})
	}

	heldPairs := pairs(held, o.MaxNonMatching, 2)
	for _, p := range heldPairs {
		if p.match {
			rep.HeldOutMatching++
		} else {
			rep.HeldOutNonMatching++
		}
	}
	rep.Default = evaluate(Default, heldPairs, o.Threshold)
	rep.Trained = evaluate(m, heldPairs, o.Threshold)
	return m, rep
}

func group(level string) string { return level[:strings.IndexByte(level, '.')] }

func clamp(p float64) float64 {
	switch {
	case p < 1e-6:
		return 1e-6
	case p > 1-1e-6:
		return 1 - 1e-6
	}
	return p
}

func heldOut(master string, share float64) bool {
	h := fnv.New32a()
	_, _ = h.Write([]byte(master))
	return float64(h.Sum32()%1000) < share*1000
}

// pairs builds every matching pair (up to 50 records per master) and a
// deterministic sample of non-matching ones.
func pairs(records []Record, maxNon int, seed int64) []pair {
	byMaster := map[string][]Attributes{}
	var masters []string
	for _, r := range records {
		if len(r.Attributes) == 0 {
			continue
		}
		if _, ok := byMaster[r.Master]; !ok {
			masters = append(masters, r.Master)
		}
		if len(byMaster[r.Master]) < 50 {
			byMaster[r.Master] = append(byMaster[r.Master], r.Attributes)
		}
	}
	sort.Strings(masters)
	var out []pair
	var all []Record
	for _, mID := range masters {
		recs := byMaster[mID]
		for i := range recs {
			all = append(all, Record{Master: mID, Attributes: recs[i]})
			for j := i + 1; j < len(recs); j++ {
				out = append(out, pair{recs[i], recs[j], true})
			}
		}
	}
	n := len(all)
	if n < 2 {
		return out
	}
	if total := n * (n - 1) / 2; total <= maxNon {
		for i := 0; i < n; i++ {
			for j := i + 1; j < n; j++ {
				if all[i].Master != all[j].Master {
					out = append(out, pair{all[i].Attributes, all[j].Attributes, false})
				}
			}
		}
		return out
	}
	rng := rand.New(rand.NewSource(seed))
	for k := 0; k < maxNon; k++ {
		i, j := rng.Intn(n), rng.Intn(n)
		if all[i].Master != all[j].Master {
			out = append(out, pair{all[i].Attributes, all[j].Attributes, false})
		}
	}
	return out
}

func evaluate(m Model, ps []pair, threshold float64) Quality {
	var tp, fp, fn int
	for _, p := range ps {
		score, _ := m.Compare(p.a, p.b)
		switch {
		case score >= threshold && p.match:
			tp++
		case score >= threshold && !p.match:
			fp++
		case p.match:
			fn++
		}
	}
	q := Quality{FalseMatches: fp}
	if tp+fp > 0 {
		q.Precision = float64(tp) / float64(tp+fp)
	}
	if tp+fn > 0 {
		q.Recall = float64(tp) / float64(tp+fn)
	}
	return q
}
