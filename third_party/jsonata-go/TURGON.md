# jsonata-go, patched for Turgon

This is [blues/jsonata-go](https://github.com/blues/jsonata-go) v1.5.4 (MIT, see LICENSE),
without its tests, test data and the jsonata-server and jsonata-test commands. Turgon's go.mod
points `github.com/blues/jsonata-go` here with a `replace`.

Two changes, both in `eval.go` and marked `Turgon:`:

- **`~>` no longer mutates the compiled expression.** `evalFunctionApplication` inserted the
  left-hand side into the arguments of the compiled function call. A second evaluation then had
  one argument too many (`$substring takes 3 arguments, got 4`). That second evaluation could be
  the next document, or the next element of a path or `$map`.
- **Built-in functions are named and given their context on a per-call copy.** Upstream set
  both on the single value every expression shares, which raced between goroutines.

When upgrading, take the new release and reapply both changes, or drop this copy if upstream
fixes them.
