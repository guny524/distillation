package runner

import (
	"encoding/json"
	"errors"
	"strings"
)

// ErrNoJSONObject means no parseable JSON object was found in a teacher
// response. The caller re-requests once (schema_retries) before giving up.
var ErrNoJSONObject = errors.New("no JSON object found in response")

// ExtractJSONObject pulls one JSON object out of a model response that may be
// wrapped in code fences or surrounded by extra prose. Strategy: try the whole
// (trimmed) string first, then decode from each '{' with json.Decoder, which
// tolerates trailing text (closing fences, commentary) after the object.
func ExtractJSONObject(s string) (map[string]any, error) {
	trimmed := strings.TrimSpace(s)
	var m map[string]any
	if err := json.Unmarshal([]byte(trimmed), &m); err == nil {
		return m, nil
	}
	for i := 0; i < len(trimmed); i++ {
		if trimmed[i] != '{' {
			continue
		}
		dec := json.NewDecoder(strings.NewReader(trimmed[i:]))
		var candidate map[string]any
		if err := dec.Decode(&candidate); err == nil {
			return candidate, nil
		}
	}
	return nil, ErrNoJSONObject
}
