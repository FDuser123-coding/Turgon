package console

import (
	"context"
	"net/http"
)

type userKey struct{}

func withUser(r *http.Request, u User) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), userKey{}, u))
}

func userFrom(r *http.Request) User {
	u, _ := r.Context().Value(userKey{}).(User)
	return u
}
