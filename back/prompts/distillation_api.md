Generate a single high-quality Q&A pair for distillation training data.

## Taxonomy (4-axis classification)

The following taxonomy defines the 4-axis classification system:
- axis 1 `domain`: 13 subject domains
- axis 2 `capability`: 8 cognitive capabilities
- axis 3 `difficulty`: 3 levels (easy, medium, hard)
- axis 4 `task_shape`: 6 output formats

```yaml
{{.TaxonomyYAML}}
```

## Current coverage

The current distribution of previously generated data across the 4 axes:

```json
{{.CoverageJSON}}
```

Identify which axis combinations are underrepresented or missing entirely. Prioritize combinations with zero or lowest counts. If `total_count` is 0 or the maps are empty, treat all combinations as equally uncovered and freely choose a combination that maximizes diversity.

## Task: generate one Q&A pair

Based on the underrepresented combination identified above, generate exactly one high-quality Q&A pair. Follow these constraints:

### 1. Task design
- Do NOT fixate on a single domain, profession, or format. Create a task that a real human user would actually ask an LLM to perform.
- Each sample must be a single task with realistic context, constraints, and success criteria that a real user would naturally include.
- Avoid template-like easy tasks or monotonous distributions. Design tasks that reveal capability differences between models.
- The task must be legally and ethically appropriate.

### 2. Answer quality
- Do NOT just provide a final answer. Include explicit problem-solving procedures that a human can learn from: `plan`, `reasoning_summary`, and `self_check`.
- Do NOT copy hidden internal chain-of-thought verbatim. Structure reasoning as explicit, externally presentable artifacts.
- If the task is verifiable, state how. If the task is ambiguous or has multiple valid answers, explicitly state the limitations and evaluation criteria.

### 3. Metadata quality
- `success_criteria`: concrete, checkable conditions that a correct answer must satisfy.
- `quality_notes`: explain why this sample has learning value and what aspects should be verified during quality review.
- `capability_tags`: select one or more capabilities from the taxonomy that this task exercises.

### 4. Good task directions
Fact explanation, comparative analysis, calculation, planning, creative writing, summarization, critique, persuasion, format conversion, problem solving, code review, tool-assisted work.

### 5. What to avoid
- Tasks that depend on external private systems or real credentials.
- Tasks with no possible answer or no evaluation criteria at all.
- Responses that are verbose but contain no substantive reasoning artifacts.

## Output format

Respond with exactly one JSON object and nothing else. No markdown code fences, no commentary, no surrounding text.

Required fields: `task_id`, `domain`, `difficulty`, `task_shape`, `capability_tags`, `user_request`, `context`, `success_criteria`, `plan`, `reasoning_summary`, `final_answer`, `self_check`, `quality_notes`

Optional fields: `references`, `artifacts`

The `task_id` field must be exactly: `{{.TaskID}}`

## Final checklist before responding

1. All 13 required fields are present.
2. `task_id` equals `{{.TaskID}}` exactly.
3. `domain` value exists in the taxonomy axis 1 enum.
4. `difficulty` value exists in the taxonomy axis 3 enum.
5. `task_shape` value exists in the taxonomy axis 4 enum.
6. Every value in `capability_tags` exists in the taxonomy axis 2 enum.
7. `success_criteria`, `plan`, `self_check`, `quality_notes` are non-empty arrays of non-empty strings.
8. `user_request`, `context`, `reasoning_summary`, `final_answer` are non-empty strings.
9. The response is one valid JSON object with no other text.
