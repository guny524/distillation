# 데이터 스키마

## 1. 레코드 (distillation.schema.json)

`distillation_pairs` 한 행 = 한 lane의 Q&A trajectory. teacher 응답은 이 JSON schema(additionalProperties: false)로 검증된 것만 적재된다.

- user_request / context / success_criteria / plan / final_answer / self_check / quality_notes: 질문과 답변 본문
- cot: 프롬프트로 강제한 in-band 사고 과정 — 모든 소스에서 존재
- cot_raw / has_raw_cot: provider가 응답의 `reasoning_content` 필드로 원본 추론을 노출하는 경우에만 존재(예: DeepSeek-R1/Qwen3 등 오픈 모델 서빙), 노출하지 않는 provider는 null
- source_lane(ko/en) / pair_id: 2-lane 쌍 묶음 — 한 질문에서 한/영 두 trajectory가 나오고 pair_id로 연결
- artifact_refs: 역생성 근거 artifact 출처(소스 링크/발췌 locator)
- difficulty_mutations: 적용된 Evol 변형(장애 주입/부분 정보 제거/상충 제약)
- student_filter_verdict / verification: presolve 채점·verify 판정 결과
- teacher_model / teacher_provider: 생성 주체 기록

## 2. 4축 분류 (../config/taxonomy.yaml)

질문 창작의 근거가 아니라 분류·coverage 관리 축이다. Go enum과 JSON schema에 미러링되어 있어 값 변경은 코드 변경이다.

- domain (13): software-engineering, data-science, mathematics, natural-science, finance, business, legal-compliance, education, creative-writing, technical-writing, linguistics, philosophy-ethics, general-knowledge
- capability (8): reasoning, knowledge-recall, generation, transformation, evaluation, planning, problem-solving, instruction-following
- difficulty (3): easy, medium, hard
- task_shape (6): short-text, long-text, code, structured-data, analysis-report, step-by-step

## 3. Parquet 반출 정책

- flush 게이트: `presolved_at AND verified_at AND superseded_at IS NULL` 통과분만 `distillation_pairs`로 투영 후 반출 — presolve와 verify는 반출의 필수 관문
- 반출된 batch는 **append-only 불변**: baseline 가중치부터 현재까지 batch를 순서대로 다시 학습하면 언제나 같은 결과가 재현돼야 하므로 소급 폐기/수정이 없다
- 재평가(soft-delete, `superseded_at`)는 아직 반출되지 않은 아이템에만 허용된다
