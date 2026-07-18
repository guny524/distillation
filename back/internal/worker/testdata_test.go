package worker

// Scripted stage responses reused across worker tests. They mirror the shapes
// the pipeline stages validate (backtranslate/mutate/student/judge/translate/
// teacher/verify), so a worker test drives real pipeline logic through the fake
// LLM. Kept minimal but schema-valid (non-empty required fields, valid enums).

const backtranslateJSON = `{
  "domain": "software-engineering",
  "difficulty": "medium",
  "task_shape": "code",
  "capability_tags": ["reasoning", "problem-solving"],
  "user_request": "Explain how to bound exploding gradients during training and implement a guard.",
  "context": "You maintain a training loop that occasionally diverges.",
  "success_criteria": ["names a clipping strategy", "handles the divergence case"],
  "reference_answer_sketch": "Clip gradient norm to a threshold before the optimizer step.",
  "why_relevant": "The source discusses stabilizing optimization, which grounds the question."
}`

const mutateJSON = `{
  "user_request": "Bound exploding gradients under conflicting throughput vs stability constraints, and implement a guard.",
  "difficulty": "hard",
  "success_criteria": ["names a clipping strategy", "resolves the throughput/stability tension"],
  "applied_mutations": ["conflicting_constraint", "not_a_real_operator"]
}`

const studentAnswerA = `{"answer": "Use gradient clipping by norm."}`

const judgePassJSON = `{"verdict": "pass", "score": 0.92}`
const judgeFailJSON = `{"verdict": "fail", "score": 0.1}`

const translateJSON = `{
  "user_request": "학습 중 폭주하는 그래디언트를 제한하고 가드를 구현하라.",
  "context": "가끔 발산하는 학습 루프.",
  "success_criteria": ["클리핑 전략 언급", "발산 처리"],
  "reference_answer_sketch": "노름 클리핑."
}`

const teacherTrajectoryJSON = `{
  "plan": ["detect divergence", "clip gradient norm", "verify stability"],
  "reasoning_summary": "Clipping the global norm bounds the update magnitude.",
  "final_answer": "Clip the gradient norm to a fixed threshold before optimizer.step().",
  "self_check": ["threshold is positive", "clip happens before step"],
  "quality_notes": ["teaches a robust stabilization pattern"]
}`

const verifyPassJSON = `{"method": "rule", "result": "pass", "detail": "criteria satisfied"}`
const verifyFailJSON = `{"method": "rule", "result": "fail", "detail": "no clipping strategy"}`

// scriptQuestionRoles scripts the generator role for a backtranslate + mutate
// pass (two generator calls).
func scriptQuestionRoles(llm *fakeLLM) *fakeLLM {
	return llm.script("generator", ok(backtranslateJSON), ok(mutateJSON))
}

// scriptPresolveToTeacher scripts a student+judge pass that routes to the teacher
// (judge fail): three student samples then a judge fail.
func scriptPresolveToTeacher(llm *fakeLLM) *fakeLLM {
	return llm.
		script("student", ok(studentAnswerA), ok(studentAnswerA), ok(studentAnswerA)).
		script("judge", ok(judgeFailJSON))
}

// scriptPresolveSolved scripts a student+judge pass where the student already
// solves it (judge pass + full agreement): item is superseded.
func scriptPresolveSolved(llm *fakeLLM) *fakeLLM {
	return llm.
		script("student", ok(studentAnswerA), ok(studentAnswerA), ok(studentAnswerA)).
		script("judge", ok(judgePassJSON))
}
