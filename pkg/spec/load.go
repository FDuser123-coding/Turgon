// Package spec decodes Turgon objects from YAML or JSON documents. Decoding
// is strict: unknown fields are rejected, so typos surface at load time
// rather than as silently ignored configuration.
package spec

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
)

// Document is one decoded object and where it came from.
type Document struct {
	Source string
	Object v1alpha1.Object
}

// Decode decodes a single YAML or JSON document into its typed object.
func Decode(data []byte) (v1alpha1.Object, error) {
	js, err := yaml.YAMLToJSON(data)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	var tm v1alpha1.TypeMeta
	if err := json.Unmarshal(js, &tm); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	if tm.APIVersion != v1alpha1.APIVersion {
		return nil, fmt.Errorf("unsupported apiVersion %q (want %q)", tm.APIVersion, v1alpha1.APIVersion)
	}
	var obj v1alpha1.Object
	switch tm.Kind {
	case v1alpha1.KindConnectorManifest:
		obj = &v1alpha1.ConnectorManifest{}
	case v1alpha1.KindConnection:
		obj = &v1alpha1.Connection{}
	case v1alpha1.KindRecipe:
		obj = &v1alpha1.Recipe{}
	case v1alpha1.KindMapping:
		obj = &v1alpha1.Mapping{}
	case v1alpha1.KindSlotContract:
		obj = &v1alpha1.SlotContract{}
	case v1alpha1.KindPlugin:
		obj = &v1alpha1.Plugin{}
	case v1alpha1.KindStackBlueprint:
		obj = &v1alpha1.StackBlueprint{}
	case v1alpha1.KindPolicyPack:
		obj = &v1alpha1.PolicyPack{}
	default:
		return nil, fmt.Errorf("unknown kind %q", tm.Kind)
	}
	dec := json.NewDecoder(bytes.NewReader(js))
	dec.DisallowUnknownFields()
	if err := dec.Decode(obj); err != nil {
		return nil, fmt.Errorf("%s: %w", tm.Kind, err)
	}
	return obj, nil
}

// DecodeAll decodes a stream of "---"-separated documents, skipping empty ones.
func DecodeAll(data []byte, source string) ([]Document, error) {
	var docs []Document
	for i, chunk := range splitDocuments(data) {
		if len(bytes.TrimSpace(stripComments(chunk))) == 0 {
			continue
		}
		obj, err := Decode(chunk)
		if err != nil {
			return nil, fmt.Errorf("%s (document %d): %w", source, i+1, err)
		}
		docs = append(docs, Document{Source: source, Object: obj})
	}
	return docs, nil
}

// LoadFile reads every document in a YAML or JSON file.
func LoadFile(path string) ([]Document, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return DecodeAll(data, path)
}

// LoadPaths loads files and, recursively, every .yaml, .yml and .json file
// under directories, skipping hidden files and directories. Results are ordered by path for reproducibility.
func LoadPaths(paths ...string) ([]Document, error) {
	var files []string
	for _, p := range paths {
		info, err := os.Stat(p)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			files = append(files, p)
			continue
		}
		err = filepath.WalkDir(p, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			// Skip hidden entries, such as the ..data directories of a
			// Kubernetes ConfigMap mount, which hold second copies.
			if path != p && strings.HasPrefix(d.Name(), ".") {
				if d.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			if !d.IsDir() && isSpecFile(path) {
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	sort.Strings(files)
	var docs []Document
	for _, f := range files {
		d, err := LoadFile(f)
		if err != nil {
			return nil, err
		}
		docs = append(docs, d...)
	}
	return docs, nil
}

func isSpecFile(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".yaml", ".yml", ".json":
		return true
	}
	return false
}

func splitDocuments(data []byte) [][]byte {
	var docs [][]byte
	var cur bytes.Buffer
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if strings.TrimRight(line, " \t") == "---" {
			docs = append(docs, append([]byte(nil), cur.Bytes()...))
			cur.Reset()
			continue
		}
		cur.WriteString(line)
		cur.WriteByte('\n')
	}
	docs = append(docs, cur.Bytes())
	return docs
}

func stripComments(b []byte) []byte {
	var out bytes.Buffer
	for _, line := range bytes.Split(b, []byte("\n")) {
		if t := bytes.TrimSpace(line); len(t) > 0 && t[0] != '#' {
			out.Write(line)
			out.WriteByte('\n')
		}
	}
	return out.Bytes()
}
