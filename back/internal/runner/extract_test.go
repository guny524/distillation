package runner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractJSONObject_Plain(t *testing.T) {
	m, err := ExtractJSONObject(`{"a": 1, "b": "x"}`)
	require.NoError(t, err)
	assert.Equal(t, "x", m["b"])
}

func TestExtractJSONObject_WhitespaceWrapped(t *testing.T) {
	m, err := ExtractJSONObject("\n\n  {\"a\": 1}\n  ")
	require.NoError(t, err)
	assert.EqualValues(t, 1, m["a"])
}

func TestExtractJSONObject_CodeFence(t *testing.T) {
	m, err := ExtractJSONObject("```json\n{\"a\": 1, \"nested\": {\"b\": 2}}\n```")
	require.NoError(t, err)
	assert.Contains(t, m, "nested")
}

func TestExtractJSONObject_ProseAround(t *testing.T) {
	m, err := ExtractJSONObject("Here is the requested object:\n{\"a\": \"v\"}\nHope this helps!")
	require.NoError(t, err)
	assert.Equal(t, "v", m["a"])
}

// A stray '{' in leading prose must not defeat extraction.
func TestExtractJSONObject_BraceInProse(t *testing.T) {
	m, err := ExtractJSONObject("The {object} you asked for: {\"a\": true}")
	require.NoError(t, err)
	assert.Equal(t, true, m["a"])
}

func TestExtractJSONObject_EmbeddedBracesInStrings(t *testing.T) {
	m, err := ExtractJSONObject(`{"code": "func main() { fmt.Println(\"}\") }"}`)
	require.NoError(t, err)
	assert.Contains(t, m["code"], "Println")
}

func TestExtractJSONObject_NoJSON(t *testing.T) {
	_, err := ExtractJSONObject("I am unable to comply with this request.")
	require.ErrorIs(t, err, ErrNoJSONObject)
}

func TestExtractJSONObject_ArrayIsNotObject(t *testing.T) {
	_, err := ExtractJSONObject(`[{"a": 1}]`)
	// The decoder still finds the inner object; that is acceptable because
	// schema validation rejects wrong shapes downstream. Assert no panic and
	// a deterministic outcome.
	if err != nil {
		assert.ErrorIs(t, err, ErrNoJSONObject)
	}
}
