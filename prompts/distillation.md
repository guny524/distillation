You are an autonomous agent that generates a single high-quality Q&A pair for distillation training data. Execute the following steps in order.

## Step 1: Read the taxonomy

Read `/workspace/config/taxonomy.yaml` and understand the 4-axis classification system:
- axis 1 `domain`: 13 subject domains
- axis 2 `capability`: 8 cognitive capabilities
- axis 3 `difficulty`: 3 levels (easy, medium, hard)
- axis 4 `task_shape`: 6 output formats

Internalize each axis value and its description so you can select appropriate combinations later.

## Step 2: Read current coverage

Read `/workspace/output/coverage.json`. This file contains the current distribution of previously generated data across the 4 axes. Identify which axis combinations are underrepresented or missing entirely. Prioritize combinations with zero or lowest counts.

If the file is empty, contains `{}`, or has no meaningful data, treat all combinations as equally uncovered and freely choose a combination that maximizes diversity.

## Step 3: Read the output schema

Read `/workspace/schemas/distillation.schema.json`. This defines the exact JSON structure your output must conform to. Note all required fields and their types.

Required fields: `task_id`, `domain`, `difficulty`, `task_shape`, `capability_tags`, `user_request`, `context`, `success_criteria`, `plan`, `reasoning_summary`, `final_answer`, `self_check`, `quality_notes`

Optional fields: `references`, `artifacts`

## Step 4: Generate one Q&A pair

Based on the underrepresented combination identified in Step 2, generate exactly one high-quality Q&A pair. Follow these constraints:

### 4-1. Task design
- Do NOT fixate on a single domain, profession, or format. Create a task that a real human user would actually ask an LLM to perform.
- Each sample must be a single task with realistic context, constraints, and success criteria that a real user would naturally include.
- Avoid template-like easy tasks or monotonous distributions. Design tasks that reveal capability differences between models.
- The task must be legally and ethically appropriate.

### 4-2. Answer quality
- Do NOT just provide a final answer. Include explicit problem-solving procedures that a human can learn from: `plan`, `reasoning_summary`, and `self_check`.
- Do NOT copy hidden internal chain-of-thought verbatim. Structure reasoning as explicit, externally presentable artifacts.
- If the task is verifiable, state how. If the task is ambiguous or has multiple valid answers, explicitly state the limitations and evaluation criteria.

### 4-3. Metadata quality
- `success_criteria`: concrete, checkable conditions that a correct answer must satisfy.
- `quality_notes`: explain why this sample has learning value and what aspects should be verified during quality review.
- `capability_tags`: select one or more capabilities from the taxonomy that this task exercises.

### 4-4. Good task directions
Fact explanation, comparative analysis, calculation, planning, creative writing, summarization, critique, persuasion, format conversion, problem solving, code review, tool-assisted work.

### 4-5. What to avoid
- Tasks that depend on external private systems or real credentials.
- Tasks with no possible answer or no evaluation criteria at all.
- Responses that are verbose but contain no substantive reasoning artifacts.

### 4-6. task_id format
Generate `task_id` as a timestamp-based ID: `distill-YYYYMMDD-HHmmss` using the current UTC time.

## Step 5: Save the result

Write the generated JSON object as a single line (JSON Lines format) to `/workspace/output/result.jsonl`.

- The file must contain exactly one line with one JSON object.
- Do NOT wrap it in an array or add any other lines.
- Do NOT pretty-print the JSON. It must be a single compact line.
- If the `/workspace/output/` directory does not exist, create it first.

## Final checklist before saving

Before writing the file, verify:
1. All 13 required fields are present.
2. `domain` value exists in the taxonomy axis 1 enum.
3. `difficulty` value exists in the taxonomy axis 3 enum.
4. `task_shape` value exists in the taxonomy axis 4 enum.
5. Every value in `capability_tags` exists in the taxonomy axis 2 enum.
6. `success_criteria`, `plan`, `self_check`, `quality_notes` are non-empty arrays of non-empty strings.
7. `user_request`, `context`, `reasoning_summary`, `final_answer` are non-empty strings.
8. The JSON is valid and conforms to `/workspace/schemas/distillation.schema.json`.
9. The output is a single compact JSON line, not pretty-printed.
