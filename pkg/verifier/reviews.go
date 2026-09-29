package verifier

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// Review decisions.
const (
	ReviewApproved = "approved"
	ReviewRejected = "rejected"
)

// Review is a person's decision on a mapped field in the review queue.
type Review struct {
	Mapping    string    `json:"mapping"` // name@version
	Target     string    `json:"target"`
	Expression string    `json:"expression,omitempty"`
	Decision   string    `json:"decision"`
	Reviewer   string    `json:"reviewer"`
	Note       string    `json:"note,omitempty"`
	At         time.Time `json:"at"`
}

// ReviewKey identifies what a review holds for: one expression of one
// field of one mapping version.
type ReviewKey struct {
	Mapping, Target, ExpressionSHA256 string
}

// Reviews are the decisions a verifier applies.
type Reviews map[ReviewKey]Review

// ExpressionDigest hashes an expression for its review key.
func ExpressionDigest(expr string) string {
	sum := sha256.Sum256([]byte(expr))
	return hex.EncodeToString(sum[:])
}

// Find returns the review of a field's current expression, if any.
func (r Reviews) Find(mapping, target, expr string) (Review, bool) {
	rv, ok := r[ReviewKey{mapping, target, ExpressionDigest(expr)}]
	return rv, ok
}

func or(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
