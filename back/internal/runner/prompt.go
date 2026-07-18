package runner

import (
	"fmt"
	"strings"
	"text/template"
)

// promptData is what the prompt template can reference.
type promptData struct {
	// TaskID is the Go-generated unique id the model must echo back.
	TaskID string
	// TaxonomyYAML is the raw content of config/taxonomy.yaml.
	TaxonomyYAML string
	// CoverageJSON is the current 4-axis coverage summary from PostgreSQL.
	CoverageJSON string
}

// systemPrompt pins the response contract: exactly one JSON object.
const systemPrompt = "You are a data generation engine. " +
	"Respond with exactly one JSON object and nothing else: " +
	"no markdown code fences, no commentary, no surrounding text."

// parsePromptTemplate compiles the prompt template once at runner startup.
func parsePromptTemplate(src string) (*template.Template, error) {
	tmpl, err := template.New("prompt").Option("missingkey=error").Parse(src)
	if err != nil {
		return nil, fmt.Errorf("parse prompt template: %w", err)
	}
	return tmpl, nil
}

// renderPrompt executes the template with per-item data.
func renderPrompt(tmpl *template.Template, data promptData) (string, error) {
	var b strings.Builder
	if err := tmpl.Execute(&b, data); err != nil {
		return "", fmt.Errorf("render prompt template: %w", err)
	}
	return b.String(), nil
}
