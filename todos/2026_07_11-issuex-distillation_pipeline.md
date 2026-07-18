# Distillation 파이프라인: artifact 역생성 + subgate 전송 + queue 상태머신
- 이슈 주소: `local-only`
- 목적: 구독제/오픈모델의 남는 quota로 frontier 모델에서 전문 도메인 distillation 데이터(질문 + 추론 trajectory)를 주기적으로 뽑아 검증·저장하는 파이프라인
- 진행 이력: 1차로 artifact 역생성 + subgate 전송 교체 + monolithic run(완료), 2차로 grilling을 거쳐 DB queue 상태머신으로 재설계(진행 중)
- **레포 경계**: 이 문서는 distillation 레포만. 게이트웨이(OAuth/CLI transport, /v1, /quota) 구현은 subgate 레포 소유(`~/Documents/Developments/subgate/todos/2026_07_11-issuex-subgate_gateway_implementation.md`). 이 레포는 subgate를 "OpenAI 호환 endpoint + GET /quota" 외부 의존으로만 취급
- 관련 문서: [research/2026-07-11_expert-domain-reasoning-trajectory-extraction-and-small-model-distillation.md](../research/2026-07-11_expert-domain-reasoning-trajectory-extraction-and-small-model-distillation.md), [cluster.md](../cluster.md)

## 1. 배경
### 1-1. 현행(1차 이전) 문제
- codex exec가 4축 taxonomy를 읽고 부족한 조합 질문을 스스로 창작 -> 자기가 답 -> result.jsonl -> entrypoint.sh가 DB 적재
- 문제1 질문 품질: 출처가 "모델의 상상"이라 전문성 미보장 (NVIDIA 페르소나+Magpie로 겪은 영양가 없는 질문과 동일)
- 문제2 quota 낭비: 제일 비싼 frontier가 출제까지 담당, student가 이미 풀 문제도 teacher에 보냄
- 문제3 quota 무지: 잔량 모른 채 고정 cron
- 문제4 proxy 결합: codex 인증/호출이 레포에 결합돼 확장·집중 불가

### 1-2. grilling 확정 핵심 사실 (2026-07-17, 실측 기반)
- **원본 CoT는 구독제에서 불가**(실측): codex `model_reasoning_summary=detailed`+`show_raw_agent_reasoning=true`도 한 줄 요약 헤더, 세션로그 encrypted_content는 Fernet(AES-128-CBC, OpenAI 키로만 복호화). claude thinking은 빈 값+암호 signature. OpenAI 문서는 raw 추출 시도를 AUP 위반으로 명시
- **logprobs 경로 불가**: 최종 답변 토큰에만, reasoning 모델은 미지원
- **원본 CoT는 오픈모델만**: reasoning_content로 raw 제공(DeepSeek-R1/Qwen3 온프레미스, Kimi 유료 API). Kimi K3는 2.8T라 홈클러스터 부적합, 7/27은 가중치 공개일(무료 API 아님)
- **소스 3종**: (a) 구독 codex/claude → subgate 경유, raw 없음(in-band 요약만), (b) 오픈모델 hosted API, (c) 온프레미스 vLLM. (b)(c)는 이미 OpenAI 호환이라 subgate 불필요, distillation이 직접 base_url 지정
- ToS: 구독 outputs를 학습 타겟으로 쓰는 건 Anthropic/OpenAI ToS 위반. 감지는 콘텐츠가 아니라 계정·접근 패턴(대량 자동화, 비공식 프록시) 기반. cli transport(공식 클라이언트)가 direct(비공식 프록시)보다 계정 리스크 낮음. 이건 사용자 리스크 결정

## 2. 요구사항
### 2-1. subgate 의존 (외부 계약, 구현은 subgate 레포)
- OpenAI 호환 `/v1/{responses,chat/completions,models}` + `GET /quota`(provider별 primary5h/secondary weekly used_percent+resets_at, cli면 unknown) + 요청헤더 `X-Subgate-Source` 태그별 usage attribution
- direct/cli transport 선택은 subgate config 소관, 이 레포는 계약에 맞춰 호출/설정만

### 2-2. 전송 계층 교체 (1차, 완료)
- internal/teacher: base_url 기반 순수 OpenAI 클라이언트, 역할별 endpoint config(역할 다른 둘이 같은 endpoint 허용)
- codex exec/entrypoint.sh/result.jsonl 파일 핸드오프 제거
- quota pacing 루프 + PostgreSQL 상태 + advisory lock

### 2-3. 질문 파이프라인 개편 (1차, 완료)
- artifact ingestion 4종: arXiv, StackExchange 덤프, k8s docs/KEP+OSS issue, PMC OA subset. 벤치마크(MedQA/GPQA) seed 금지(오염)
- 역생성(Instruction Backtranslation/Bonito): artifact 기반 (질문+근거+참조답안 스케치)
- 난이도 상승: Evol-Instruct를 출제자 아닌 변형 연산자로(장애주입/부분정보제거/상충제약)
- presolve(구 "student 사전 필터"): local student가 미리 풀어봐 이미 풀면 폐기, 못 풀면 통과 (quota 절약)
- 2-lane 언어: 한/영 쌍, 한쪽만 있으면 judge 번역, 각 lane에서 해당 언어로 CoT·content
- verifier: 코드는 실제 실행, 나머지는 rule/none. 통과분만 저장
- 4축 taxonomy는 창작 근거가 아니라 분류/coverage 축으로

### 2-4. quota pacing (완료, 상세 sec 5-4)
- 사용자 reserve 불필요(많이 쓰면 이번 시간 생성분이 자동 감소가 보호), 주간 reset까지 남은 시간으로 주간 budget을 시간 배분해 그 1시간 한도까지 소진

### 2-5. 재설계: DB queue 상태머신 (2차, grilling 확정, 진행 중)
- monolithic `distill run`(생성→필터→답변→검증 일괄)을 DB 큐 기반 단계별 worker로 분해
- 이유: presolve on/off, soft-delete/재평가, 큰 소스(arXiv 전문) 처리, 단계별 재시도·재개, 부분 운영이 monolithic에선 불가
- reasoning 캡처 확장: 구독은 요약이라도 in-band로, 오픈모델은 raw까지 (2-5-2)

#### 2-5-1. 상태 = 단계별 nullable timestamp column (순차 채움)
- `queued_at → comprehended_at → questioned_at → presolved_at → answered_at → verified_at → flushed_at`
- soft-delete는 `superseded_at`(flush 전 아이템만), 실패는 `failed_at/fail_stage/fail_reason`
- 어느 단계까지 왔나 = 어느 timestamp까지 non-null인가. worker는 "다음 단계 null AND 이전 단계 non-null" row를 claim

#### 2-5-2. 락 (사용자 확정)
- `lock_owner_id`(POD_NAME/hostname/기동시 UUID) + `lock_at` 1쌍만(단계별 아님, 재활용). claim할 때만 `SELECT ... FOR UPDATE SKIP LOCKED`로 짧은 트랜잭션(후보 좁힘+안 잠긴 것), UPDATE로 owner/at 세팅 후 즉시 commit(락 해제). 긴 작업(comprehend/answer)은 트랜잭션 밖. 완료 후 단계 timestamp 채우고 owner/at NULL. stale(lock_at 오래됨)이면 다른 worker 회수. answer는 추가로 quota pacing 게이트 통과해야 claim

#### 2-5-3. worker 2종 병렬 (사용자 확정)
- always-on Deployment(replica, 큐 폴링): answer/verify 등 정상 소진. replica는 POD_NAME으로 owner 구분(statefulset 불필요)
- 정기 CronJob/ofelia: ingest(소스 발견)/flush(반출)
- comprehend: opencode agentic worker

#### 2-5-4. comprehend (큰 소스, 사용자 확정)
- plain chat API는 auto-compact 없음(초과=400 에러)·링크 못 읽음. CLI harness(opencode/codex)만 fetch+auto-compact
- opencode agentic worker가 소스 링크 fetch → 읽기 → auto-compact → 이해 → 질문 생성. subgate 바라봄(구독 소비=목적 부합, 질문 생성엔 raw CoT 불필요)
- opencode: image에 Dockerfile로 설치, 설정(provider/baseURL=subgate, agent 정의)은 configmap/kustomization

#### 2-5-5. reasoning 캡처 (grilling 확정)
- `cot`(프롬프트로 강제한 in-band 사고, 전 소스 항상) + `cot_raw`(응답 reasoning_content, 오픈모델만) + `has_raw_cot`(bool)
- 분기 없음: 프롬프트 항상 cot key 강제, teacher client가 reasoning_content 있으면 cot_raw에. provider 몰라도 됨. 기존 reasoning_summary는 cot로 대체/명확화

#### 2-5-6. flush = append-only 스냅샷 (사용자 확정 B1)
- flush 게이트: `presolved_at AND verified_at AND superseded_at IS NULL AND flushed_at IS NULL`. presolve는 flush 전 필수 관문(student 없으면 DB에 대기, 안 나감)
- 나간 parquet는 **불변**(재현성: baseline→현재 가중치 순차 재현학습이 깨지면 안 됨). 소급 폐기 없음. soft-delete는 flush 전 아이템만
- flush = 기존 append parquet + DB 제거 그대로. canonical/tombstone(codex B2안) 불필요

#### 2-5-7. ingest idempotency (codex 지적, 실재 결함)
- 현재 arXiv start=0 고정, k8s pagination TODO, task_id timestamp라 같은 문서 매 run 재적재
- `UNIQUE(source_type, doc_id)` + 소스별 cursor/watermark를 큐 테이블/별도 테이블에

#### 2-5-8. 배포 (kube + docker-compose, kream_crawl ofelia 패턴)
- 단일 바이너리 subcommand(ingest/comprehend/answer/verify/flush): kube는 정기=CronJob·상시=Deployment, docker-compose는 ofelia daemon(labels `@every Nm`)로 정기 잡 스폰+상시 worker는 compose 서비스. 단일 이미지 재사용
- 참고: `~/Documents/Developments/kream_crawl` deployments/overlays/docker-compose(ofelia), research의 SELECT FOR UPDATE SKIP LOCKED·CNPG

### 2-6. 배포별 config 주입 (완료, 재빌드 없이 오버라이드)
- overlays/ 축은 "배포 환경"(prod/docker-compose)뿐, "모델 소싱 정책"은 config 값(잘못 만든 overlays/local-llm 폐기)
- 역할별 endpoint: `SUBGATE_ENDPOINT_<ROLE>` env override(짧은 스칼라, ConfigMap mount 대신)
- prompt 템플릿: distill-prompt ConfigMap(prompts/distillation_api.md 단일 소스 mount). taxonomy/schema는 Go enum·JSON schema 결합이라 baked
- `make apply`(overlays/prod/Makefile)가 `--load-restrictor LoadRestrictionsNone` 감쌈(prompt가 base 밖 참조)

## 3. 기존 코드/구현의 핵심 (현재 monolithic 구현체)
- internal/teacher: 역할별 OpenAI 클라이언트(재시도/타임아웃/source 태그), ChatCompletion이 message.content만 반환(reasoning_content 폐기 — 2-5-5에서 교체)
- internal/pacing: Decide()/UpdateEMA()/PGStore + migrations/0001_quota_pacing.sql(4테이블)
- internal/runner: monolithic run(lock→quota→Decide→k회 생성→EMA). 재설계로 단계별 worker로 분해 대상
- internal/pipeline: backtranslate/mutate/studentfilter/lane/verify/executor. stage 로직은 살리되 인라인 호출→큐 worker로 재배치
- internal/ingest: 4소스 어댑터 + Registry(FetchAll). cursor/idempotency 없음(2-5-7)
- internal/flush: append parquet→DB 제거(재사용), internal/exporter, internal/loader, internal/coverage
- schemas/distillation.schema.json: additionalProperties:false, reasoning_summary만(cot/cot_raw 없음 — 2-5-5)

## 4. 생각한 수정 방안들
- 전송 계층: Go 네이티브 전면 전환 확정(codex exec 유지/사이드카 기각)
- 질문 구조: artifact 역생성 + taxonomy 하위 유지 확정
- 실행 구조: (1차) monolithic run → (2차) DB queue 상태머신 확정. codex는 "홈클러스터 규모엔 과설계, resumable monolith 권장"이나 사용자가 단계 독립운영·agent comprehend·재평가를 요구해 queue 채택. codex의 실재 결함(ingest idempotency, reasoning 계약, flush supersede)만 반영
- flush 재평가: B1(append-only, 소급 폐기 없음) 확정, codex B2(canonical/tombstone) 기각

## 5. 최종 결정된 수정 방안
### 5-1. 1차 monolithic (완료, grilling 승인)
- M1 subgate(별도 레포) / M2 전송 교체 / M3 질문 파이프라인 / M4 coverage·export. 상세 완료 기록은 sec 8

### 5-2. 2차 재설계: DB queue 상태머신 (grilling 확정, 진행 중)
- 아키텍처 전체는 sec 2-5. 요지: timestamp 상태머신 + lock_owner_id/lock_at claim(SKIP LOCKED) + always-on/정기 worker 2종 + opencode comprehend + presolve 게이트 + cot/cot_raw + append-only flush + ingest idempotency + ofelia 배포
- 기존 pipeline stage 로직(backtranslate/mutate/presolve/lane/verify) 재사용, monolithic runner를 worker로 분해

### 5-3. reasoning 캡처 계약 변경 (2차)
- teacher client 반환형 string → {Content, ReasoningContent, ProviderMetadata}
- schema/model/loader/exporter 동일 migration에서 cot/cot_raw/has_raw_cot 동기화
- cot(in-band, 전 소스)과 기존 reasoning_summary 의미 통합

### 5-4. quota pacing 상세 (완료, 설계 근거 보존)
- 정책: weekly deadline-aware, `hourly_allowance=(weekly_cap-weekly_used%)/hours_left_until_weekly_reset`, 매 결정 재계산 자가보정
- 5h primary는 hard guardrail: `projected_cost <= min(hourly_allowance, primary_headroom)`, weekly보다 항상 우선
- clamp: hours_left 축소 시 allowance 폭증 방지(min horizon/시간당 최대)
- 건당 %비용은 생성 전후 used_percent delta의 EMA(alpha 0.2, span~9). EMA outlier 6종: negative_delta/rollover/too_large_delta/user_mixed_delta(attribution 없을 때 window delta)/zero_resolution_pending/crash_recovery_mixed → 제외
- attribution: subgate X-Subgate-Source 태그별 pipeline-only usage로 사용자 소비와 분리
- reset 직전 가속: reset N시간 전 horizon 축소, primary cap 불가침
- 상태: PostgreSQL 4테이블(run_logs/cost_ema/observations/hourly_budget), advisory lock, crash recovery(stale running→crashed_recovered, EMA 제외)
- config 기본값: weekly_cap_pct 100, primary_cap_pct 95, primary_safety_margin 1, min_horizon_hours 1, max_per_hour_pct 10, accel_window_hours 24, accel_horizon_ratio 0.5, reset_blackout_seconds 300, max_items_per_run 5, cli_unknown(quota 없는 provider) virtual budget 별도
- DDL/의사코드 전문은 internal/pacing 및 migrations/0001_quota_pacing.sql에 구현됨(git diff로 확인)

## 6. 코드 수정 요약 (checkbox + 검증 방법)
### 6-1. 1차 monolithic (완료 — 상세 검증 기록 sec 8)
- [x] M2 전송 계층(internal/teacher + runner + cmd/distill run + Dockerfile/cronjob 재구성)
- [x] M3 스키마 확장 + 4소스 ingestion + 역생성/변형/presolve/2-lane/verify(internal/pipeline) + verifier 실제 실행
- [x] M4 coverage/export + ingest→runner 배선
- [x] 배포별 config 주입(env override + prompt ConfigMap + make apply)

### 6-2. 2차 재설계 (진행 중)
- [x] 큐 테이블 스키마 + timestamp 상태머신 + UNIQUE(source_type,doc_id) + lock_owner_id/lock_at 마이그레이션 / 검증: 마이그레이션 + 상태 전이 단위 테스트 (internal/queue, 별도 distillation_items 테이블 채택)
- [x] claim 로직(SELECT FOR UPDATE SKIP LOCKED 짧은 트랜잭션 + stale 회수) / 검증: 동시 2 worker claim 겹침 없음 + stale 회수 테스트 (단일 UPDATE...WHERE id=(SELECT FOR UPDATE SKIP LOCKED)로 자동커밋 짧은 트랜잭션)
- [x] ingest cursor/watermark + idempotent upsert / 검증: 같은 소스 재실행 시 중복 등록 없음 (Enqueue ON CONFLICT(source_type,doc_id) DO NOTHING + distillation_source_cursors)
- [x] reasoning 계약 변경(client 반환형 + schema/model/loader/exporter cot/cot_raw/has_raw_cot 동기화) / 검증: reasoning_content 있는 응답 파싱 + 없는 응답 폴백 (teacher.Complete 신설, ChatCompletion은 content-only wrapper 유지)
- [x] stage worker 분해(comprehend/question/presolve/answer/verify + ingest/flush를 큐 worker로) / 검증: 각 단계 fake Store + fake LLM + fake pacing으로 상태 전이(Claim->Complete/Fail/Supersede) 단위 테스트 (internal/worker, pipeline 스테이지 로직 재사용: Backtranslate/Mutate/Presolve/LaneRecords/VerifyRecord 노출)
- [x] comprehend opencode worker(소스 fetch+auto-compact) + Dockerfile opencode 설치 + opencode config 파일 / 검증: fake commandRunner로 성공→digest 추출/Complete, exec 실패→transient Retry, 빈 출력→Retry, disabled→provenance 폴백 단위 테스트 (make lint/lint-schema/test -race 19패키지/build PASS) — 근거: ComprehendWorker가 주입된 Comprehender 사용, OpencodeComprehender가 `opencode run --agent comprehend --model subgate/gpt-5.4 --auto <prompt>` 서브프로세스로 소스 URL fetch+auto-compact 후 `<comprehension_digest>` 태그 span 추출, exec/빈출력은 teacher.ErrTransient로 감싸 Retry, config.Comprehend.Opencode.Enabled=false면 ProvenanceComprehender 폴백. opencode 설정(provider=subgate baseURL, comprehend agent webfetch/read 허용·write/edit/bash deny)은 config/opencode.json에 작성, Dockerfile은 musl 릴리스(opencode-linux-x64-musl.tar.gz) 설치. 잔여: k8s configMapGenerator로 config/opencode.json mount + always-on Deployment는 다음 배포 단계(deployments/ 미변경)
- [x] presolve를 flush 전 게이트로(on/off, 미통과 soft-delete) / 검증: presolve off면 pass-through(Complete)·on이면 게이트 통과분만 진행 (미통과=student 이미 풀면 superseded). presolved_at은 flush 게이트 필수 관문이라 off여도 stamp. transient-retry(일시 장애 시 폐기 말고 재시도)는 retry_count/next_attempt_at 컬럼으로 도입 완료(하단 하드닝 #4)
- [x] flush 게이트 갱신(presolved+verified+미supersede+미projected) / 검증: 게이트 조건별 반출 여부 (flush worker가 Claim(StageFlush)로 게이트 통과분만 project. 하드닝 #2로 lane INSERT+projected_at 마킹을 단일 트랜잭션·owner fenced로, projected_at(buffer)과 flushed_at(parquet) 분리, 기존 internal/flush로 parquet 반출)
- [x] 상시 worker 하드닝: 교차검토 확정 6결함(#1 pacing 정합성·#2 flush 원자성·#3 payload 방어·#4 transient-retry·#5 excerpt 보존·#6 lock fencing+stale lease) / 검증: 하단 sec 8 "2차 재설계 worker 하드닝" 상세 + 신규 단위 테스트 (make lint/lint-schema/test -race 19패키지/build PASS)
- [x] always-on Deployment worker + 정기 CronJob/ofelia 분리 배포 + docker-compose overlay(ofelia) / 검증: `kubectl kustomize --load-restrictor LoadRestrictionsNone deployments/base`+`.../overlays/prod` PASS(17 리소스), `docker compose -f deployments/overlays/docker-compose/docker-compose.yml config` PASS(9 서비스), 오프라인 시맨틱 체크(selector=template label 일치·command·schedule) PASS — deployments/base/workers.yaml(always-on 5 Deployment `distill <stage> --loop`) + cronjob.yaml repurpose(정기 2 CronJob ingest */30·flush hourly) + kustomization(distill-opencode configMap + secret-opencode) + overlays/prod 갱신 + overlays/docker-compose(ofelia). 단일 이미지 재사용, POD_NAME downward API로 lock_owner_id fencing, wait-for-postgres initContainer가 self-migrate readiness 게이트. 상세 sec 8

### 6-3. worker 교차 검토 확정 결함 수정 (2026-07-18, codex 3중 검토 + grep 재확인, make 4종 green 직접 검증)
- [x] #6 lock fencing (HIGH, 키스톤): Complete/Fail/Retry/Supersede(claim.go) + SetPayload/SetAnswerPayload/ProjectAndComplete(store.go) 전부 `AND lock_owner_id=$owner` fencing, 0 rows면 ErrLeaseLost로 처리 중단(중복 write 금지). stale lease 15m→90m 상향 + config화(WorkerConfig.StaleLockMinutes, heartbeat는 pgx.Conn 비동시성으로 기각) / 검증: TestFencing_StaleReclaimedWorkerCannotComplete, TestFencing_LeaseLostMidStageSkipsItem, TestComplete/Fail/Retry/Supersede_LeaseLost
- [x] #4 에러 재시도 분류 (HIGH): retry_count/next_attempt_at/last_error 컬럼(0002 마이그레이션), teacher.ErrTransient(429/5xx/네트워크/timeout 소진 시)는 Retry(backoff·락 해제), 계약/파싱 오류·재시도 소진은 terminal Fail, claim 술어에 `(next_attempt_at IS NULL OR next_attempt_at<=now())` 게이트 / 검증: TestRetry_TransientErrorReschedules, TestRetry_ContractErrorFailsTerminally, TestRetry_ExhaustionFailsTerminally, TestClaimSQL_RetryGate
- [x] #1 pacing 정합성 (HIGH): (b)EMA 분모를 spent 아이템으로 통일(호출≠아이템), (c)lane.go가 res,err 반환해 파싱 실패해도 실제 teacherCalls 정산, (d)cli_unknown run-log generated_count로 virtual budget 감소, (a)advisory lock으로 replica 직렬화 후 hourly budget 원자적 reserve+미사용 반환 / 검증: TestPacingGate_EMAIsPerItem, TestAnswer_PartialFailureSettlesConsumption, TestPacingGate_CLIUnknownRecordsUsage, TestPacingGate_DirectReservesThenReconciles
- [x] #2 flush 원자성 (HIGH): ProjectAndComplete가 lane INSERT+projected_at 마킹을 owner-fenced 단일 트랜잭션(부분 project 불가, lease-lost시 전체 rollback), projected_at(buffer)/flushed_at(parquet) 분리, StageFlush done-col·Supersede 가드가 projected_at 기준, 실제 parquet은 기존 buffer 기반 internal/flush 유지 / 검증: TestFlush_LeaseLostProjectsNothing, TestSupersede_SoftDeletesPreProjection
- [x] #5 artifact 발췌 보존 (HIGH): Enqueue가 ingest 발췌를 source_excerpt 컬럼에 보존, buildDigest가 title/URL only가 아닌 발췌 근거 사용(opencode 실연동은 6-2 checkbox 잔여) / 검증: TestEnqueue_Inserts(source_excerpt 포함)
- [x] #3 payload 방어 (MED): validate.go read boundary가 빈 digest/불완전 Question/빈 answer 배열을 계약(terminal) 오류로 거부(일시 아님, 무한 재시도 방지) / 검증: TestPayloadValidation_EmptyDigestRejected, TestPayloadValidation_IncompleteQuestionRejected
#### 6-3-1. codex 2차 교차검토 판정 (2026-07-18)
- CLOSED 확정: #2 flush 원자성(단일 트랜잭션 rollback), #5 excerpt(source_excerpt scan→buildDigest 실사용)
- 잔존/회귀 발견(아래 6-3-2에서 수정): #6 owner 고유성, #4 retry_count stage간 누적+408, #1 replica pacing(primary 미예약/cli 쿨다운/EMA 이중계산)
- 검증 방식: 1차 fix는 make 4종 green으로 landed, 2차가 다중 replica·stage 경계 회귀를 정련

#### 6-3-2. codex 2차 확정 결함 재수정 (2026-07-18 완료, make 4종 green 직접 검증)
- [x] D1 retry_count Complete 리셋 (HIGH, queue/claim.go completeSQL): 완료 SQL에 `retry_count = 0, last_error = ''` 추가. retry_count는 item-global 1컬럼이라 한 stage에서 쓴 재시도 카운트가 다음 stage로 누적돼 RetryPolicy.MaxRetries가 후속 stage를 첫 실오류에 조기 terminal-fail시키던 것을, 각 stage 완료 시 예산 리셋(단계별 독립)으로 교정. fencing($2) 유지 / 검증: TestComplete_ResetsRetryBudget(SQL에 retry_count=0·last_error=''·owner fence 확인)
- [x] D2 408 retryable (LOW, teacher/client.go doOnce): `>=300` non-retryable arm 앞에 `resp.StatusCode == http.StatusRequestTimeout` case 추가(retryable=true). 408은 일시성 타이밍 신호라 영구 실패 부적절, 409/425 등 타 4xx는 범위 밖 유지 / 검증: TestChatCompletion_RetryOn408(재시도 후 복구), TestChatCompletion_408ExhaustedIsTransient(소진 시 IsTransient·"unexpected status" 아님)
- [x] D3 owner 프로세스 고유성 (MEDIUM, worker/worker.go ResolveWorkerID): base(POD_NAME/hostname)에 프로세스별 random 8byte suffix 항상 append(`<base>-<rand8>`), rand 실패 시 PID 폴백(살아있는 PID는 호스트 내 고유). 같은 host 두 프로세스가 hostname 공유 시 owner 충돌→ABA로 fencing 무력화되던 것 차단. 호출부는 시작 시 1회 저장(claim~complete는 item.lock_owner_id readback으로 안정) / 검증: TestResolveWorkerID_SameBaseDistinct(같은 base 두 owner 상이), TestResolveWorkerID_PodName(suffix append 확인)
- [x] D4 unlock ctx 누수 방지 (MEDIUM, worker/pacer.go): advisoryUnlock 헬퍼가 `context.Background()`+5s timeout로 unlock, 에러 로깅(무시 금지). Budget defer와 Settle EMA-lock 양쪽에 적용. 취소된 pass ctx로 unlock하면 세션 advisory lock이 커넥션 reap까지 누수돼 모든 후속 pacing 결정이 wedge되던 것 차단. PGStore는 단일 conn이라 ctx만 바꿔도 같은 세션에서 안전 / 검증: TestPacingGate_* 계열이 unlock 경로 실행(회귀 없음, race clean)
- [x] D5 primary headroom 대칭 예약 — **예약 계열 삭제로 소멸**(6-3-4 무상태 게이트 확정): reservation ledger 자체가 제거돼 "replica 간 primary 예약" 개념이 사라짐. primary(5h)는 이제 무상태 hard-ceiling `used_percent >= primary_cap_pct - safety_margin`(worker/pacer.go StatusGate.directExhausted)로만 판정. multi-replica within-tick overshoot는 다음 /quota 사실로 자동 보정(사용자 sec 2-4 설계) / 검증: TestStatusGate_PrimaryCeiling(93%<94% 통과·94%/96% skip)
- [x] D6 cli-unknown 429 쿨다운 — **in-memory 쿨다운으로 재구현**(DB run-log 제거): Settle/InsertRunLog/StatusRateLimited 경로 삭제, 대신 StatusGate가 프로세스 내 `cooldownUntil[provider]` 타임스탬프 유지. answerOne이 teacher.ErrRateLimited 관측 시 rateLimited 반환 → RunOnce가 pass 중단 + gate.NoteRateLimited()로 쿨다운(CLIUnknownRateLimitCooldown 기본 1h) arming → 쿨다운 동안 Allow가 cli-unknown provider를 skip. DB 기록 0 / 검증: TestStatusGate_RateLimitCooldownSkipsUnknown(429→skip→만료 후 재개), TestAnswer_RateLimitedStopsPassAndArmsCooldown(pass 중단+gate arming)
- [x] D7 EMA 학습 lock 직렬화 — **EMA/advisory lock 삭제로 소멸**: 건당 비용 EMA 학습·advisory lock 직렬화 전부 제거(잔량 진실은 /quota 사실뿐, distillation DB에 소비 기록 금지). 겹친 관측창 이중계상 문제 자체가 근본 소멸(학습할 상태가 없음) / 검증: 해당 없음(삭제된 서브시스템)
- [x] D8 question read boundary 필수필드+enum (MEDIUM, worker/validate.go): validateQuestion에 reference_answer_sketch(answer 단계 lane.go teacherPrompt가 참조) non-empty, capability_tags non-empty 추가, model.ValidateEnums 재사용해 domain/difficulty/task_shape/capability_tags enum 검증. 계약(terminal) 오류로 분류해 loader INSERT(flush) 전에 teacher quota 낭비 없이 거부. 생성부 validateClassification 계약을 읽기 경계에 대칭 반영(persisted JSONB 손상 방어) / 검증: TestValidateQuestion_RejectsMissingReferenceSketch/RejectsMissingCapabilityTags/RejectsInvalidEnums(4 enum 하위케이스)/AcceptsValid
- 추가 마이그레이션: 없음. D1(retry_count/last_error)·D5(quota_hourly_budget_consumption의 window_kind='primary')·D6(quota_pacing_run_logs.status='rate_limited', LastRateLimitedAt)는 전부 기존 컬럼/status 재사용이라 0003 불요
- 시그니처 변경(호출부 동기화): Pacer.Settle에 rateLimited bool 인자 추가(answer.go 인터페이스·pacer.go 구현·fake_test/pacer_test 동기화). subgate·monolithic runner·comprehend 배선(worker.go buildComprehender/NewComprehendWorker) 무변경
- **후속(6-3-4)**: 위 D1~D8 중 D5/D6/D7이 속한 예약 원장(Pacer.Budget/Settle/reserve/EMA/advisory lock) 서브시스템 전체가 사용자 grilling으로 기각·삭제됨. 이 절의 예약 관련 기술은 역사적 기록이며 현재 구현은 6-3-4 무상태 게이트(worker.Gate/StatusGate)임

#### 6-3-3. pacing 예약 원장 설계 결정 필요 (codex 3차 판정 + 사용자 승인 대기)
codex 3차 수렴 리뷰: D1·D2·D3·D4·D8 CLOSED 확정, 그러나 D5/D6/D7 미해결 + 새 결함 3개가 **전부 "예약 원장(reservation ledger)" 한 서브시스템에 집중**. NOT CONVERGED.
- 근본 원인: 예약 원장은 codex 1-2차의 multi-replica 과claim 지적을 막으려 도입했으나, **사용자 sec 2-4 설계("사용자 reserve 불필요, 많이 쓰면 이번 시간 생성분 자동 감소가 보호")와 정면 배치**. /quota 스냅샷 피드백 위에 원장을 얹어 이중 정산이 구조적으로 발생
- codex 3차 잔존/신규(전부 pacer.go/decide.go 예약 로직):
  - D5 REGRESSED: 시간 경계·비원자적 예약·snapshot 중복차감으로 quota accounting 붕괴
  - 신규 HIGH: Budget hour에 예약·Settle hour에 reconcile → hour-crossing pass가 이전 예약 orphan + 새 hour primary fence 소실
  - 신규 HIGH: secondary/primary 예약이 별도 SQL → primary 쓰기 실패 시 secondary phantom 예약 잔류(비원자)
  - 신규 MEDIUM/D5동일: settled primary가 ledger 잔류 → /quota refresh 후 snapshot·ledger 이중차감 → **hour rollover 전 잘못된 K=0(single replica에서도 quota 미소진, 프로젝트 목적 "짜내기" 위반)**
  - D6 STILL-OPEN: 취소 ctx로 Settle → run log가 decided 잔류·예약 미반환
  - D7 STILL-OPEN: `after` 스냅샷을 lock 밖에서 읽어 겹친 관측창 이중계상
- **설계 fork(사용자 결정)**:
  - (A) 원장 제거 + sec 2-4 피드백 복귀(권장): Decide가 /quota 스냅샷만 단일 진리로 K 계산, 예약 원장 삭제 → D5·신규3개 근본 소멸(single-replica 미소진 버그 해결), multi-replica 내 within-tick overshoot는 다음 /quota로 자동 보정(사용자 설계 그대로). D6 쿨다운·per-item EMA는 유지(피드백에도 필요), D7은 EMA 학습 단순화
  - (B) 원장 유지 + hardening: 예약·정산을 단일 트랜잭션·hour-boundary 정합·snapshot-vs-ledger 단일화. multi-replica within-tick 엄격 상한이 필요하고 answer replica>1 운영 시에만 값. 복잡도 높고 라운드마다 edge case 재발 이력
  - 권장 A: sec 2-4 설계 충실 + 근본 해결 + 현재 배포(replicas:1)에 정확. CORE(quota 정산) 변경이라 자율 미실행, 사용자 승인 후 진행
- **결정·구현 완료(2026-07-18)**: 사용자가 grilling으로 (A) 원장 제거를 확정(6-3-4). worker/pacer.go의 PacingGate/Budget/Settle/reserve/EMA/advisory lock 전면 삭제 -> 무상태 StatusGate로 재작성. D5·D6·D7 및 신규 3결함(hour-crossing orphan·비원자 예약·snapshot-vs-ledger 이중차감)이 예약 원장 삭제로 근본 소멸. FAIL이던 TestPacingGate_PrimaryReservationPreventsOverClaim은 예약 테스트째 삭제되어 해소

#### 6-3-4. pacing grilling 결정 로그 (2026-07-18, 무상태 게이트 구현 완료)
- **기각(사용자)**: "쓸 예정을 DB에 기록(예약)" 방식 자체 기각 — 잔량의 진실은 외부(subgate GET /quota = 구독 status)뿐, distillation DB에 소비 예정 기록 금지
- **확정(사용자)**: step별 각자 quota 확인 — role endpoint에 quota_url 있으면(=subgate 경유 구독) claim 전 확인해서 소진 시 그 step만 안 돌고, 없으면(=진짜 API) 그냥 쓰다가 에러(429/insufficient_quota)로 처리. step이 병렬이어도 각자 확인이라 문제없음
- **확정(사용자)**: endpoint 사실 — role 6종(teacher/generator/judge/translator/verifier/student) 중 **student만 local, 나머지 전부 subgate 경유 구독**. step->확인 role: comprehend->opencode provider, question->generator, presolve->judge, answer->teacher+translator, verify->verifier
- **사실 확인**: GET /quota는 OpenAI 호환 표준 아님(표준엔 잔액/quota endpoint 없음, 소진 시 429 insufficient_quota 등 에러-driven) — subgate 독자 계약. RoleConfig.QuotaURL 필드는 기존재(현재 teacher만 채움)
- **범위 확정(사용자)**: 이 flow 정합을 위해 subgate 레포 수정도 이번 작업 범위에 포함, grilling하며 안 맞으면 양 레포 수정
- **sqlc 확인**: 미사용(sqlc.yaml 없음, 수기 SQL 상수+migrations/*.sql을 기동 시 적용). pacing 확정·반영 후 전환 여부 별도 결정
- **확정(사용자)**: 기준선 판단은 (ii) — worker가 /quota **사실만** 읽고 스스로 판단. 근거(사용자, SOLID): subgate 책임은 구독의 OpenAI 호환 변환뿐이라 pace 판정을 endpoint 쪽에 두면 표준에 없는 필드로 호환 표면이 오염되고, quota path 자체가 없는 진짜 API로 role을 돌리면 판단 로직이 증발 — 소비 계획은 소비자(distillation) 소관. subgate 무변경(/quota 사실 계약 유지, subgate todo sec 5에 기록). AI의 i 추천은 철회(cli 트래픽 가시성은 "사실 수집" 근거지 "판단 위치" 근거가 아니었음)
- **지시(사용자) -> 완료(2026-07-18)**: sqlc 도입 — sqlc.yaml + Makefile(`make sqlc`/`sqlc-check`) 세팅, 정적 쿼리(queue Enqueue/cursor, loader count/maxID/delete) 생성 전환, 동적 stage 컬럼 SQL·frozen 제약 InsertRecord는 수기 유지(사유·해제조건 기록). 상세 sec 8 "sqlc 도입"

##### 6-3-4-1. 무상태 per-step 게이트 구현 완료 (2026-07-18)
- 위 결정을 그대로 구현: 예약 원장 전면 삭제 + subgate GET /quota 사실만 읽는 무상태 게이트로 재작성. distillation DB에 소비 예정/EMA/예약/run-log 기록 0
- [x] StatusGate 신설(internal/worker/pacer.go): worker.Gate 인터페이스(Allow/NoteRateLimited) + StatusGate 구현. 판정 공식 — secondary(주간) 직선 페이싱 `used_percent >= weekly_cap_pct × elapsedRatio(resets_at, 168h)`(sec 2-4 "남은 budget/남은 시간" 장기 등가, 무상태·주석 명시), primary(5h) 무배분 상한 `used_percent >= primary_cap_pct - safety_margin`, 둘 중 하나라도 걸리면 skip. cli-unknown(used_percent null)은 사실 없어 통과+429 쿨다운 지배, fetch/parse 실패·provider 부재는 보수적 skip. 상한/마진/주기/쿨다운 전부 기존 pacing.Config 재사용 / 검증: TestStatusGate_SecondaryBelowTargetAllows·SecondaryOverTargetSkips·SecondaryBoundarySkips·PrimaryCeiling·CLIUnknownAllows·NoChecksAllowsWithoutFetch·FetchFailureSkips·ParseFailureSkips·ProviderAbsentSkips·RateLimitCooldownSkipsUnknown·CooldownIgnoredForDirect·DedupSharedURL·AnyExhaustedRoleSkips·TestElapsedRatio
- [x] in-memory 쿨다운(DB 기록 금지): StatusGate.cooldownUntil[provider] 프로세스 내 타임스탬프. NoteRateLimited가 arming, Allow가 cli-unknown provider에 대해서만 소비. 429 관측 시 answer는 아이템 Retry(기존)+게이트 arming+pass 중단 / 검증: TestStatusGate_RateLimitCooldownSkipsUnknown, TestAnswer_RateLimitedStopsPassAndArmsCooldown
- [x] step별 각자 확인 배선(cmd/distill/worker.go stageQuotaChecks + worker.QuotaChecksFor): comprehend->opencode provider, question->generator, presolve->judge(student local 미확인), answer->teacher+translator(둘 다), verify->verifier. quota_url 없는 role(student)은 check 미생성=확인 자체 없음. 같은 quota_url 중복은 Allow 내 1회 fetch / 검증: TestStageQuotaChecks_WiresRolesPerStep(각 step의 role 집합·student 전 stage 미포함), TestStatusGate_DedupSharedURL
- [x] 예약 계열 전면 삭제: PacingGate/Budget/Settle/reserve/reconcile/learnEMAUnderLock/settleCLIUnknown/advisoryUnlock + Pacer 인터페이스 제거. answer.go는 `게이트 통과? -> claim -> 처리` 루프로 재작성(budget/settle 루프 제거), 429면 아이템 Retry+쿨다운 arming+pass 중단. question/presolve/verify/comprehend도 gatedDrain(claim 전 Allow)로 게이트 주입. pacing 패키지(ParseSnapshot·Config·스냅샷 타입만 import)는 무변경 / 검증: 예약 테스트(pacer_test.go) 전량 삭제·재작성, FAIL이던 TestPacingGate_PrimaryReservationPreventsOverClaim 해소
- config/role 변경: teacher.RoleConfig에 QuotaProvider 추가(/quota providers[] 매칭 키), teacher.Client.FetchQuotaURL(URL 기반 GET, 게이트용) 신설, runner.OpencodeConfig에 QuotaURL/QuotaProvider 추가. settings.yaml의 모든 subgate role(teacher/generator/judge/translator/verifier)+comprehend.opencode에 quota_url+quota_provider 채움, student는 quota_url 없음(local). pacing 블록에 게이트가 쓰는 키(secondary_window_hours·primary_safety_margin_pct·cli_unknown_rate_limit_cooldown) 명시+주석
- subgate 레포: 무변경(/quota 사실 계약 유지가 결정). 판정은 소비자(distillation) 소관

## 7. 문제 해결에 참고한 자료
- 리서치 보고서(artifact 역생성, presolve, 검증 가능 trajectory, 저작권/contamination): research/2026-07-11_...md
- openai-oauth 프로토콜: https://github.com/EvanZhouDev/openai-oauth
- CoT 실측: codex/claude CLI --json·세션로그·Fernet blob 해부(원본 불가 확정), OpenAI reasoning 가이드/Azure 문서(raw 미노출·AUP), Anthropic extended-thinking(요약만)
- Kimi K3: OpenAI 호환 API(reasoning_content 제공, 유료), 7/27 가중치 공개
- opencode: options.baseURL로 커스텀 endpoint, agentic harness가 fetch+compact 제공
- kream_crawl: ofelia cron 패턴(docker-compose labels로 CronJob 등가), SELECT FOR UPDATE SKIP LOCKED, CNPG
- quota pacing: codex 의견(token bucket/deadline-aware/EDF, EMA attribution 함정) + 직접 검증
- cluster.md: 로컬 추론 워커 배치(3080/3060Ti/1080Ti가 comprehend/presolve/verify 담당, teacher는 오픈모델이면 raw CoT)

## 8. 수정사항 요약 (완료분 상세)
### M2 전송 계층 교체 (2026-07-12)
- [x] internal/teacher 신규: 역할별 OpenAI 호환 endpoint 클라이언트
  - config.go: RoleConfig(base_url/model/api_key_env/quota_url/source_tag) + HTTPConfig, 역할 다른 둘이 같은 endpoint 허용
  - client.go: /v1/chat/completions 단건, X-Subgate-Source 헤더, api_key_env Bearer, 429/5xx/네트워크 지수 backoff, 429 소진 ErrRateLimited, FetchQuota raw bytes(파싱은 runner의 pacing.ParseSnapshot)
- [x] internal/runner 신규: run 오케스트레이션(teacher/quota/DB 인터페이스 주입 mock 가능)
  - RecoverStaleRunning → TryAdvisoryLock(실패 시 skipped_lock_busy 정상 종료) → /quota → pacing.Decide → k회 생성 → /quota 재조회 → UpdateEMA(primary는 pipeline delta attribution) → AddHourlyConsumption → run log completed/rate_limited
  - task_id Go 생성(distill-YYYYMMDD-HHmmss-NN) 유일성, extract.go 견고 JSON 추출 + schema_retries 재요청, config.go settings.yaml overlay
- [x] cmd/distill run subcommand(coverage/load/export 유지), 시작 시 pacing.Migrate+CreateTable idempotent, 단일 pgx 연결로 advisory lock 세션 공유
- [x] prompts/distillation_api.md 신규(파일 I/O 지시 제거, taxonomy/coverage/task_id Go 템플릿 삽입)
- [x] 인프라: Dockerfile codex 계층 제거, cronjob.yaml 단일 컨테이너, settings.yaml 실 config화, README 재서술
- 검증: make lint/lint-schema/test(9패키지 201 테스트 -race)/build PASS. 삭제 대상(참조 0, rm은 사용자): entrypoint.sh, prompts/distillation.md

### M3 스키마 확장 (2026-07-13)
- [x] schema: M3 필드 8종(source_lane/pair_id/artifact_refs/student_filter_verdict/verification/difficulty_mutations/teacher_model/teacher_provider) + $defs 3종 + dependentRequired, 기존 required·additionalProperties:false 불변으로 legacy valid
- [x] model: DistillationPair + ArtifactRef/StudentFilterVerdict/Verification struct, enum 맵 7종, RecordToParams(M3 optional, JSONB native 유지), ValidateEnums(present일 때만 enum+중첩 required)
- [x] loader: 8컬럼 nullable(JSONB 3/TEXT 4/TEXT[] 1), CREATE IF NOT EXISTS + ALTER ADD COLUMN IF NOT EXISTS 한 Exec, INSERT $16..$23
- [x] coverage: SourceLane/ArtifactSource/LaneXSource 3축(artifact만 집계, jsonb_array_elements), cmd coverage에 idempotent CreateTable
- [x] exporter: Arrow 24필드, JSONB `::text` String export, nullable AppendNull
- 검증: make lint/lint-schema/test(9패키지 -race)/build PASS, 신규 24테스트

### M3 artifact ingestion 4소스 (2026-07-13)
- [x] internal/ingest/common: HTTPDoer(mock seam)+GetBytes, StripHTML/Excerpt(발췌 상한 단일 강제점 DefaultMaxExcerptChars=480), IsBenchmarkContaminated/FilterContaminated(벤치마크 40여종 토큰경계+MCQ/answer-key 정규식), LicensePermitsCommercial(NC/ND·미상 배제)
- [x] arxiv(Atom/dump xml, 카테고리 필터, doc_id 버전strip, abstract 발췌), stackexchange(Posts.xml streaming, 질문만, CC BY-SA, min_score), k8s(GitHub issues/PR REST, doc_id owner/repo#num, PR 구분), pmc(JATS/NXML, PMCID, CC deed href, only_commercial_use 배제)
- [x] Registry(settings.yaml ingest 섹션, enabled만 BuildEnabled/FetchAll 소스별 에러 격리, common은 leaf라 부모 패키지에 위치)
- 검증: go vet/build/lint-schema PASS, internal/ingest 6패키지 34테스트(-race). 실네트워크 TODO: arXiv per-paper license(bulk JSON), k8s Link 페이지네이션+KEP 마크다운(← 2-5-7 idempotency와 함께 재설계에서 처리)

### M3 질문 파이프라인 (역생성→필터→2-lane→verify) (2026-07-13)
- [x] internal/pipeline 신규: artifact → backtranslate → mutate → student pre-filter → 2-lane translate + teacher trajectory → verify → []DistillationPair, 모든 LLM 호출 pipeline.LLM 인터페이스(teacher.Client 만족)
  - backtranslate(generator: 질문+context+success_criteria+reference_answer_sketch+why_relevant, 4축 enum 검증, Hangul 언어감지), mutate(Evol 변형 연산자만, difficulty_mutations 기록), studentfilter(self-consistency+judge 참조답안, fail/uncertain만 teacher로), lane(translator로 반대 lane+pair_id, 각 lane 해당 언어 trajectory, TeacherCalls 계수), verify(method/result, none=na, fail만 배제), pipeline.Run+ToParams+ArtifactSource/SliceSource
- [x] internal/runner 연결: Config.Pipeline(mode/self_consistency_k/threshold/mutations/roles), DefaultConfig mode=artifact, Validate 역할 5종 강제, generateOne 분기(artifact/taxonomy), errNoMoreArtifacts sentinel. settings.yaml pipeline 섹션+M3 역할 5종
- [x] verifier 실제 코드 실행 (2026-07-13): executor.go SubprocessExecutor(temp dir+RemoveAll, Setpgid 프로세스그룹 SIGKILL, capWriter 출력상한, env 시크릿 차단, sandbox_command 훅으로 네트워크 격리 위임), 기본 disabled, verify 라우팅(코드펜스+지원언어+enabled면 실제 실행 exit0=pass, 아니면 LLM 폴백), runner toPipelineConfig 배선. TOCTOU 직접 검증
- 검증: make lint/lint-schema/test(16패키지 -race)/build PASS, 신규 26테스트

### 데이터 flush (DB→NAS, 2026-07-13)
- [x] internal/flush: MaybeFlush(threshold)→Flush(NAS 락→MAX(id) 스냅샷→FetchRowsUpTo→temp dir Parquet→fsync+VerifyParquetDir→원자적 rename→내보낸 task_id로 DELETE), run.go가 r.Run 후 호출
- [x] TOCTOU 유실 수정(직접 리뷰 발견): DELETE를 id<=maxID → task_id=ANY(내보낸 집합)으로. fetch 후 늦게 커밋된 id<=maxID row 유실 차단(task_id UNIQUE). 크래시 시 중복 재export(유실 아님) 의도적
- 검증: flush_test(disabled/threshold/스냅샷 이월/쓰기실패 무삭제) + loader DeleteByTaskIDs + exporter, make PASS(17패키지)

### 배포별 config 주입 (2026-07-13)
- [x] `SUBGATE_ENDPOINT_<ROLE>` env override(6역할, TestApplyEndpointEnvOverrides), distill-prompt ConfigMap(단일 소스 mount), overlays/prod/Makefile `make apply`(load-restrictor 파이프 감쌈)

### 2차 재설계 기반 계층: 큐 상태머신 + reasoning 계약 (2026-07-17)
- 큐 테이블 판단: distillation_pairs 확장이 아닌 별도 `distillation_items` 채택. 근거: 아이템은 answer 단계 전까지 domain/user_request/final_answer 등 distillation_pairs의 NOT NULL 계약을 채울 수 없어 placeholder row·coverage/export 오염을 강제하게 됨. 완성 산출물(distillation_pairs)과 진행 중 작업(items)은 생명주기가 달라 분리
- [x] internal/queue 신규
  - migrations/0001_queue.sql: distillation_items(provenance 6열 + 상태 timestamp queued_at..flushed_at 7종 + superseded_at/failed_at/fail_stage/fail_reason + lock_owner_id/lock_at + UNIQUE(source_type,doc_id)) + 단계별 pending 부분 인덱스 6종 + distillation_source_cursors(소스별 opaque watermark). CREATE IF NOT EXISTS만(신규 테이블), 재적용 idempotent
  - queue.go: Stage enum(comprehend/question/presolve/answer/verify/flush) → prev/done timestamp column 매핑을 단일 stages 테이블로, Item struct, Store(q+stale 보유), Migrate
  - claim.go: Claim(stage, workerID) = 단일 `UPDATE ... WHERE id=(SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING`(자동커밋=짧은 트랜잭션, 병렬 worker 비겹침, `lock_owner_id IS NULL OR lock_at < now()-make_interval(secs=>$2)`로 stale 회수). Complete(done timestamp=now + lock 해제), Fail(failed_at/fail_stage/fail_reason + lock 해제). 컬럼명은 고정 내부 테이블에서만 보간(injection-safe), workerID/stale/reason/itemID는 파라미터
  - enqueue.go: Enqueue(artifact.Artifact) = ON CONFLICT(source_type,doc_id) DO NOTHING(중복 소스 문서 무시), GetCursor/SaveCursor(재실행 시 앞부분 재수집 방지)
- [x] reasoning 계약(2-5-5/5-3): teacher.Complete(ctx,role,msgs) → `Completion{Content, ReasoningContent, Provider{Model,FinishReason}}` 신설, chatResponse에 `message.reasoning_content` 파싱(없으면 "" 폴백), HasRawCoT(). 기존 ChatCompletion(string)은 Complete().Content wrapper로 남겨 monolithic runner/pipeline 호출부 무수정
- [x] schema/model/loader/exporter cot/cot_raw/has_raw_cot 동기화: schema 3필드 추가(cot string / cot_raw ["string","null"] / has_raw_cot bool, 전부 optional이라 legacy record valid, reasoning_summary는 병존+마이그레이션 경로 주석), model DistillationPair+RecordToParams(nullable, has_raw_cot bool 형검사), loader DDL/ALTER ADD COLUMN IF NOT EXISTS/INSERT $24..$26, exporter Arrow 24→27필드(cot/cot_raw String + has_raw_cot Boolean, 전부 nullable)
- 검증: make lint/lint-schema/test(-race, 18패키지)/build PASS. 신규 테스트: queue claim이 이전단계 non-null+이번단계 null만 집음·stale 회수·Complete/Fail 상태 전이·Enqueue UNIQUE 충돌 no-op·cursor·Migrate DDL, teacher reasoning_content 있는/없는 파싱, model/loader/exporter cot 3필드 라운드트립. 기존 flush/exporter 24열 fixture를 27열로 동기화. 삭제 대상 없음(기존 monolithic 미변경, teacher 반환형은 wrapper로 호환)
- 다음 단계(worker 분해) API 매핑: comprehend/question/presolve/answer/verify worker는 `Store.Claim(stage, POD_NAME)` → 작업 → `Complete`/`Fail`, answer worker는 Claim 전 pacing.Decide 게이트 추가, ingest worker는 `Enqueue`+`GetCursor`/`SaveCursor`, flush worker는 stages[StageFlush] 게이트(presolved+verified+미supersede+미flush)로 배치 조회 후 answered payload를 distillation_pairs(cot/cot_raw/has_raw_cot 포함)로 투영. 스테이지 payload 컬럼은 worker 단계에서 정확한 형태로 추가(현재 items는 identity/provenance/state/lock만)

### 2차 재설계 stage worker 분해 (2026-07-17)
- internal/worker 신규: monolithic runner를 큐 worker 7종으로 분해, 각 worker = Claim -> 처리 -> Complete/Fail/Supersede 루프. pipeline 스테이지 로직 재사용(인라인 호출을 worker로 이동)
  - ingest(정기): Registry.BuildEnabled로 소스 fetch -> Enqueue(ON CONFLICT idempotent) -> SaveCursor. 소스별 에러 격리. cursor는 last doc_id watermark 저장(adapter incremental fetch 소비는 다음 단계 TODO, 현재는 ON CONFLICT dedup)
  - comprehend(뼈대, always-on): Claim -> provenance(title/url/locator)로 comprehension_digest 생성 -> Complete. opencode fetch+auto-compact 실연동은 다음 단계 TODO 주석
  - question(always-on): digest로 artifact 재구성 -> pipeline.Backtranslate + Mutate -> question JSONB 저장 -> Complete
  - presolve(always-on): pipeline.Presolve(studentfilter) -> toTeacher면 verdict 저장+Complete, student 이미 풀면 verdict 저장+Supersede(soft-delete). disabled면 pass-through Complete(presolved_at은 flush 필수 관문이라 stamp)
  - answer(always-on, pacing 게이트): Pacer.Budget(=pacing.Decide) 0이면 claim 안 함, budget 내 Claim -> pipeline.LaneRecords(ko/en trajectory + cot/cot_raw/has_raw_cot) -> answer_payload 저장 -> Complete -> Pacer.Settle(=quota 재조회 + UpdateEMA + 시간당 leaky bucket)
  - verify(always-on): pipeline.VerifyRecord로 lane별 검증 -> verification JSONB 저장, 한 lane이라도 pass/na면 Complete, 전부 fail이면 Supersede(폐기)
  - flush(정기): Claim(StageFlush) 게이트(presolved+verified+미supersede+미flush)로 통과분만 project, kept lane(verification != fail)을 pipeline.ToParams -> distillation_pairs INSERT(ON CONFLICT idempotent) -> Complete(flushed_at) -> 기존 internal/flush로 buffer -> NAS parquet 반출
- distillation_items payload 컬럼 5종 nullable ALTER 추가(internal/worker/migrations/0001_item_payloads.sql, queue.Migrate 후 적용): comprehension_digest TEXT / question JSONB / presolve_verdict JSONB / answer_payload JSONB / verification JSONB. 각 단계 산출물을 다음 단계가 읽음
- queue 최소 확장: `Store.Supersede`(superseded_at 마킹 + flushed_at IS NULL 가드로 append-only snapshot 보호) 1종. 나머지 queue 기반(Claim/Complete/Fail/Enqueue/cursor)은 무변경
- pipeline 확장: 스테이지 exported wrapper(Backtranslate/Mutate/Presolve/LaneRecords/VerifyRecord/Kept), teacherTrajectory reasoning-aware화(ReasoningLLM 인터페이스로 teacher.Client의 Complete 사용해 cot=in-band reasoning 항상, cot_raw/has_raw_cot는 reasoning_content 있을 때). 기존 content-only 경로/테스트 무변경
- cmd/distill: `distill <stage>` subcommand 7종(ingest/comprehend/question/presolve/answer/verify/flush). always-on(comprehend/question/presolve/answer/verify)은 --loop/--poll-interval, 정기(ingest/flush)는 1회. workerID는 POD_NAME -> hostname -> random hex. monolithic `distill run`은 유지하되 deprecated 주석(배포 전환 후 제거 대상)
- 검증: make lint/lint-schema/test(-race, 19패키지)/build PASS. 신규 테스트(internal/worker): ingest idempotency/cursor/소스에러격리, comprehend digest/빈provenance fail, question backtranslate+mutate/LLM에러 fail, presolve toTeacher Complete·solved Supersede·disabled pass-through, answer quota0 no-claim·budget내 생성(cot/cot_raw 캡처)·budget cap·question누락 fail, verify anypass Complete·allfail Supersede, flush kept lane project+buffer flush·superseded/flushed 제외·length mismatch fail, worker ID/artifact재구성/drain/payload migration/JSONB cast. queue Supersede(pre-flush 가드), cmd 7 worker subcommand 등록+loop flag
- 다음 단계 인계점: (1) comprehend opencode 실연동(fetch+auto-compact, Dockerfile opencode 설치, configmap) — #5로 excerpt 근거는 확보, 실 fetch만 남음, (2) always-on Deployment + 정기 CronJob/ofelia 분리 배포 매니페스트 + docker-compose overlay, (3) answer pacing replica 조율은 하드닝 #1a(advisory lock 직렬화 + 원자적 reserve)로 도입 완료, 남은 것: per-observation run-log 영속화·cli_unknown 크래시 orphan reserve 조기 회수(현재 window rollover까지 conservative 유지), (4) ingest cursor를 adapter Fetch에 전달해 재스캔 대신 incremental 수집, (5) transient-retry는 하드닝 #4로 도입 완료(retry_count/next_attempt_at/last_error + teacher.ErrTransient 분류)

### 2차 재설계 worker 하드닝 (교차검토 확정 6결함) (2026-07-18)
- 큐 worker 구현을 codex 교차검토 + grep 재확인으로 확정한 6개 실재 결함(전부 파일:라인 근거) 수정. 상시 worker의 동시성/재시도/정합성 정합성 확보
- #6 lock fencing + stale lease (키스톤): Complete/Fail/Supersede/Retry + 모든 payload write + flush 투영의 UPDATE WHERE에 `AND lock_owner_id = $owner` 추가, affected rows=0이면 `queue.ErrLeaseLost` 반환. stale 회수된 worker의 늦은 write가 새 owner 결과를 덮어쓰지 못함. worker는 lease-lost 감지 시 아이템 처리 중단(중복 write 없음, drain은 계속). stale lease를 15m -> 90m로 상향(worst-case 단계 처리시간 초과) + `worker.stale_lock_minutes` config 노출. lock_at heartbeat는 pgx.Conn 동시성 비안전(전용 커넥션 필요)이라 이관, fencing이 정합성 backstop
- #4 transient vs terminal: distillation_items에 retry_count/next_attempt_at/last_error 컬럼(0002 마이그레이션 nullable ALTER) + claim 술어에 `(next_attempt_at IS NULL OR next_attempt_at <= now())`. 일시 오류(네트워크/429/5xx/timeout, teacher.ErrTransient sentinel 신설)면 retry_count++·backoff 후 재시도(lock 해제, Fail 아님), 계약/파싱 오류·재시도 초과면 terminal Fail. teacher client가 retryable 소진 시 ErrTransient wrap
- #1 pacing 정합성: (b) EMA 분모를 teacherCalls -> spent 아이템으로 통일(cost/item = Decide의 k 단위 일치), (c) lane.go가 teacher 호출 후 파싱 실패해도 TeacherCalls 보존(`return res` 아닌 `laneResult{}` 폐기 제거) -> 이미 발생한 소비 정산, (d) cli_unknown도 run-log generated_count로 사용량 기록해 virtual budget 감소, (a) Budget이 advisory lock으로 replica 간 결정 직렬화 + hourly budget/run-log 원자적 reserve, Settle이 실제 소비로 reconcile(미사용분 반환). answer worker에 spent(호출 발생 아이템 수) 추적 추가
- #2 flush 원자성: flush worker의 lane INSERT + 마킹을 단일 트랜잭션(`Store.ProjectAndComplete`, owner fenced)으로 -> 부분 투영과 done 마킹 공존 불가, 재시작 시 재투영(parquet 중복) 없음. projected_at(buffer 투영)과 flushed_at(parquet 반출) 분리: StageFlush done 컬럼을 flushed_at -> projected_at으로, Supersede 가드도 projected_at IS NULL로. 실제 parquet 반출은 기존 internal/flush가 buffer 기준(이미 원자적)
- #5 artifact 원문 손실(최소): ingest의 짧은 excerpt Chunks를 source_excerpt 컬럼에 보존(Enqueue), comprehend digest를 title/URL only가 아닌 excerpt 근거로 생성. opencode 실 fetch+compact는 다음 checkbox 유지

### comprehend opencode 실연동 (2026-07-18)
- comprehend worker를 스켈레톤(provenance-only digest)에서 opencode agentic 실연동으로 승격. `ComprehendWorker`가 주입된 `Comprehender` 인터페이스를 통해 digest 생성 -> 큐 상태 전이 계약(Claim(StageComprehend) -> SetComprehension(digest, owner) -> Complete(owner), 전부 owner fenced)은 무변경
- Comprehender 2구현(internal/worker/comprehend.go):
  - OpencodeComprehender(프로덕션): `opencode run --agent comprehend --model subgate/gpt-5.4 --auto <prompt>` 서브프로세스로 소스 URL을 fetch+auto-compact+이해 후 digest 수신. 출력은 `<comprehension_digest>...</comprehension_digest>` 태그 span을 추출(태그 없으면 trim 전체 폴백). exec 실패(바이너리 없음/비정상 종료/timeout)와 빈 출력은 teacher.ErrTransient로 감싸 handleStageError가 Retry(backoff), MaxRetries 초과 시에만 terminal Fail(조용한 품질 저하 대신 소리내어 실패)
  - ProvenanceComprehender(폴백): 기존 buildDigest(excerpt+provenance), 빈 provenance는 terminal Fail. opencode 미설치/비활성 환경(로컬/CI)에서 스테이지 유지
  - exec는 commandRunner 인터페이스로 추상화(execRunner=os/exec) -> 단위 테스트가 fake로 실 바이너리 없이 성공/transient/빈출력 검증
- 배선: cmd/distill/worker.go buildComprehender가 config.Comprehend.Opencode.Enabled로 구현 선택. baseURL/agent 등 endpoint는 바이너리에 하드코딩 금지 -> opencode config 파일(config/opencode.json)에, CLI knob(bin_path/config_path/model/agent/timeout)은 settings.yaml comprehend.opencode에. config_path는 OPENCODE_CONFIG 환경변수로 서브프로세스에 주입
- config/opencode.json: provider subgate(@ai-sdk/openai-compatible, baseURL http://svc-subgate.subgate:8080/v1, apiKey {env:OPENCODE_SUBGATE_API_KEY}), model subgate/gpt-5.4, agent comprehend(webfetch/read allow·write/edit/bash deny, 소스 fetch 후 태그 digest만 출력하는 system prompt). 구독 소비=목적 부합, 질문 생성엔 raw CoT 불필요. 다음 배포 단계에서 k8s configMapGenerator로 mount
- Dockerfile: opencode musl 릴리스(anomalyco/opencode v1.18.3, opencode-linux-x64-musl.tar.gz -> alpine 호환) 설치, libgcc/libstdc++ 추가, HOME=/home/distill(opencode 상태 기록), OPENCODE_SUBGATE_API_KEY 플레이스홀더 기본값(subgate가 실 auth 보유, k8s Secret로 override 가능). Go static binary 빌드는 무변경
- opencode CLI 리서치 근거(공식 문서 실측): `opencode run [message..]` 비대화형(opencode.ai/docs/cli), config는 opencode.json + OPENCODE_CONFIG env(opencode.ai/docs/config), 커스텀 OpenAI 호환 provider는 npm @ai-sdk/openai-compatible + options.baseURL(opencode.ai/docs/providers), agent는 permission으로 tool 허용/거부(opencode.ai/docs/agents), 설치는 install script가 /etc/alpine-release 감지해 musl 타겟 선택(릴리스에 opencode-linux-x64-musl.tar.gz 존재)
- 검증: make lint/lint-schema/test(-race, 19패키지)/build PASS. 신규 테스트(internal/worker/comprehend_test.go): opencode 성공→태그 추출/Complete+argv/env 검증, exec 실패→transient Retry, 빈출력→Retry, transient 분류, disabled→provenance 폴백, extractDigest/buildComprehendPrompt 단위. runner/config_test.go: comprehend 기본값/Timeout·YAML overlay·shipped settings 검증. 남은 불확실성: opencode 바이너리의 컨테이너 내 실행(musl 런타임 의존성)과 subgate provider apiKey 요구 여부는 이미지 빌드/실행 시 확인 필요(코드 계약은 fake로 검증됨, 실 바이너리 e2e는 배포 단계)
- #3 stage payload 방어: store read boundary에서 non-empty digest / Question 필수필드 / non-empty answer 배열 검증(validateDigest/validateQuestion/validateAnswerRecords), 실패 시 일시 아닌 계약 오류로 terminal 처리
- 마이그레이션: internal/db/migrations/queue_0002_fencing_retry_projected.sql (source_excerpt/projected_at/retry_count/next_attempt_at/last_error ALTER IF NOT EXISTS + flush pending 인덱스를 projected_at 기준으로 재생성). queue.Migrate가 0001+0002 순차 적용
- 시그니처 변경(호출부 동기화): queue.Store Complete/Fail/Supersede에 owner 추가 + Retry 신설, worker.Store payload write에 owner 추가 + ProjectAndComplete 신설, Pacer.Settle(spent, teacherCalls). subgate/monolithic runner 무변경
- 검증: make lint/lint-schema/test(-race, 19패키지)/build PASS. 신규 테스트: queue fencing(Complete/Fail/Retry/Supersede lease-lost) + Retry SQL + next_attempt_at claim gate + source_excerpt enqueue, worker fencing(stale 회수 worker write 거부·mid-stage lease-lost skip) + transient 재시도 vs 계약 terminal + 재시도 초과 terminal + payload 빈값 거부 + 부분실패 소비 정산 + flush 단일 트랜잭션 project·lease-lost 무투영·projected_at 게이트, pacing gate(lock-busy skip·direct reserve+reconcile·EMA 아이템 단위·cli_unknown 사용량 기록)

### 2차 확정 결함 재수정 (D1~D8) (2026-07-18)
- codex 2차 교차검토가 1차 fix의 잔존/회귀로 확정한 8개 실재 결함(전부 파일:라인 근거) 수정. 다중 replica pacing 정합성·stage 경계 재시도 예산·owner 고유성·읽기 경계 계약을 근본 교정. 상세 결함별 전후/근거/테스트는 sec 6-3-2
- D1(queue/claim.go): Complete가 retry_count/last_error 리셋 → stage별 재시도 예산 독립(누적으로 후속 stage 조기 fail 차단)
- D2(teacher/client.go): 408 Request Timeout을 retryable로 → 일시적 타임아웃이 영구 실패로 오분류되던 것 교정
- D3(worker/worker.go): ResolveWorkerID가 프로세스별 random suffix 항상 append(rand 실패 시 PID) → 같은 host 두 프로세스 owner 충돌(ABA) 차단
- D4(worker/pacer.go): advisory unlock을 fresh context.Background()+timeout으로+에러 로깅 → 취소 ctx로 인한 세션 lock 누수 차단
- D5(worker/pacer.go+pacing/decide.go): primary window 대칭 예약+reconcile, Decide가 primary hourly ledger 구독 → replica들이 같은 /quota로 primary cap 초과 claim하던 것 차단(quota 절약 방향의 보수성)
- D6(worker/answer.go+pacer.go): cli-unknown 429 관측 시 pass 중단+run log StatusRateLimited 기록 → 기존 LastRateLimitedAt 쿨다운 발동, 다음 Budget k=0(연속 429 방지)
- D7(worker/pacer.go): EMA 학습을 Settle의 advisory lock 재획득 아래로(busy면 skip+폴백 reconcile) → 겹친 관측창에서 전역 delta 이중 귀속으로 EMA 부풀리던 것 차단, reconcile은 lock-free로 병렬성 보존
- D8(worker/validate.go): question read boundary에 reference_answer_sketch/capability_tags non-empty + ValidateEnums enum 검증 → teacher quota 지출 전 계약 오류 terminal 거부
- 설계 재량 근거: D5 예약 방식(hourly ledger 구독, /quota 반영 후 보수성만 남아 과소비 없음), D6 쿨다운 창(config CLIUnknownRateLimitCooldown 기본 1h 재사용), D7 lock 재획득(TryAdvisoryLock 비차단, busy면 학습만 skip·정산은 계속) 모두 코드 주석+본 요약에 명시
- 마이그레이션 없음(전부 기존 컬럼/status 재사용). Pacer.Settle에 rateLimited 인자만 추가(호출부 동기화). comprehend 배선·subgate·monolithic runner 무변경
- 검증: make lint/lint-schema/test(-race, 19패키지)/build PASS. 신규 테스트 12종(D1~D8 결함별 + D3/D6 보강): TestComplete_ResetsRetryBudget, TestChatCompletion_RetryOn408/408ExhaustedIsTransient, TestResolveWorkerID_SameBaseDistinct, TestPacingGate_PrimaryReservationPreventsOverClaim/CLIUnknownRateLimitCooldown/SettleEMASkippedWhenLockBusy, TestAnswer_RateLimitedStopsPassAndSignalsCooldown, TestValidateQuestion_*(4). 기존 pacer/answer 테스트는 Pacer.Settle 시그니처만 동기화(회귀 없음)

### 배포 분리: monolithic CronJob -> 큐 worker 분리 배포 (2026-07-18)
- monolithic `distill run` CronJob을 큐 worker 분리 배포로 전환(sec 2-5-3/2-5-8). deployments/ 밑만 수정, 단일 이미지(distillation:latest) 재사용, backward-compat 없이 monolithic 흔적 교체
- deployments/base/workers.yaml 신규: always-on Deployment 5종(comprehend/question/presolve/answer/verify), 각 `command: [distill, <stage>, --loop]`. 공통 = envFrom secret-postgres + POSTGRES_HOST=svc-postgres/POSTGRES_DB=distillation + POD_NAME(downward API fieldRef metadata.name -> ResolveWorkerID의 lock_owner_id, 각 replica 고유 fencing) + SUBGATE_ENDPOINT_* 6종(빈값=baked) + distill-prompt subPath mount + wait-for-postgres initContainer(postgres:15-alpine pg_isready 루프). replicas 1 기본(answer는 advisory-lock 직렬화+fencing으로 스케일 가능 주석). 공통 label component=worker(prod 패치 타겟)
- comprehend 추가 배선(계약: sec 2-5-4 + opencode 실연동): distill-opencode configMap을 /workspace/config/opencode.json에 subPath mount(baked 위 override) + OPENCODE_CONFIG env=그 경로 + HOME=/home/distill(opencode 상태 dir) + OPENCODE_SUBGATE_API_KEY(secretKeyRef secret-opencode) + 상향 리소스(250m/256Mi req, 1/1Gi limit — opencode Bun 서브프로세스가 큰 소스 fetch)
- deployments/base/cronjob.yaml repurpose(monolithic 제거): 정기 CronJob 2종. distillation-ingest(schedule */30, 소스 발견+Enqueue, 근거: metadata fetch+ON CONFLICT dedup 저렴), distillation-flush(schedule hourly, presolved+verified 게이트 투영+buffer NAS export, pvc-distillation-nfs를 /mnt/nfs/distillation에 mount + POD_NAME fencing). 둘 다 concurrencyPolicy Forbid / restartPolicy OnFailure / activeDeadlineSeconds 1800 / backoffLimit 2 / wait-for-postgres init
- 마이그레이션 결정: cmd/distill에 migrate 서브커맨드 없음 확인 -> 워커 openDB(cmd/distill/worker.go)가 기동 시 idempotent self-migrate(pacing/loader/queue/payload), wait-for-postgres initContainer가 readiness 게이트. 동시 DDL은 CREATE/ALTER IF NOT EXISTS + k8s 재시작으로 수렴(별도 migrate 잡 불필요)
- deployments/base/kustomization.yaml: resources에 workers.yaml 추가(cronjob.yaml 유지), configMapGenerator distill-opencode(opencode.json=../../config/opencode.json), secretGenerator secret-opencode(opencode.env=커밋된 placeholder — subgate가 실 auth 보유라 non-secret, secret-*.env gitignore 회피 위해 opencode.env 명명). disableNameSuffixHash 유지
- deployments/overlays/prod/kustomization.yaml: 삭제된 monolithic CronJob을 참조하던 stale 로컬-LLM 예시 패치를 worker Deployment 타겟(role->worker 매핑: question=generator, presolve=student+judge, answer=translator, verify=verifier)으로 갱신
- deployments/overlays/docker-compose/ 신규(kream_crawl ofelia 패턴): docker-compose.yml — db(postgres:15-alpine, pg_isready healthcheck, .env 비밀) / migrate(build 레포루트 Dockerfile + `distill ingest`로 self-migrate 게이트, service_completed_successfully로 워커 차단) / nas-init(NAS 볼륨 chown 1001, flush 비-root 쓰기) / always-on worker 5종(`distill <stage> --loop`, hostname=service명=고유 owner) / ofelia(daemon --docker labels로 ingest @every 30m·flush @every 1h, pull=false 로컬이미지, environment/volume JSON배열, 비밀은 .env 보간). project/network/volume 이름 고정. .env.example(POSTGRES_PASSWORD 필수, OPENCODE_SUBGATE_API_KEY 선택)
- opencode config mount 방식(sec 2-6 no-rebuild 일관): k8s=configMapGenerator distill-opencode -> subPath mount, compose=bind mount(../../../config/opencode.json:ro). 양쪽 OPENCODE_CONFIG env가 경로 지정, api key는 secret-opencode(k8s)/.env(compose). endpoint(baseURL=subgate)/agent는 config 파일에만, 바이너리 하드코딩 없음
- 검증: `kubectl kustomize --load-restrictor LoadRestrictionsNone deployments/base` PASS, `.../overlays/prod` PASS(17 리소스: 5 worker+postgres Deployment, ingest/flush CronJob, 2 ConfigMap, 2 Secret, PV/2 PVC, Service, Namespace), `docker compose -f deployments/overlays/docker-compose/docker-compose.yml config` PASS(9 서비스). 오프라인 시맨틱 체크(Deployment selector=template label 일치, workload command, CronJob schedule) PASS. secret-postgres 해시명 전파·secret-opencode/configMap 참조 정합 확인. 남은 불확실성: 실 클러스터 apply와 opencode 바이너리 컨테이너 e2e는 이미지 빌드/배포 시 확인(매니페스트 계약은 kustomize 렌더+파싱으로 검증, kubeconform 미설치=옵션)

### quota pacing 무상태 전환 (예약 원장 -> per-step /quota 사실 게이트) (2026-07-18)
- 결정 근거·공식·삭제 목록은 sec 6-3-4/6-3-4-1. grilling으로 예약 원장(reserve/EMA/advisory lock/run-log)이 사용자 sec 2-4 설계와 정면 배치됨을 확인, 전면 삭제하고 subgate GET /quota 사실만 읽는 무상태 per-step 게이트로 재작성
- 게이트 공식(무상태): secondary(주간) 직선 페이싱 `used% >= weekly_cap_pct × elapsedRatio(resets_at,168h)`(sec 2-4 장기 등가), primary(5h) 무배분 상한 `used% >= primary_cap_pct - safety_margin`, 하나라도 걸리면 그 step만 skip. cli-unknown(used% null)은 사실 없어 통과, 429(teacher.ErrRateLimited) 관측 시 프로세스 내 in-memory 쿨다운(DB 기록 0) 동안 skip. fetch/parse 실패·provider 부재는 보수적 skip
- step별 각자 확인: comprehend->opencode provider, question->generator, presolve->judge(student local 미확인), answer->teacher+translator, verify->verifier. quota_url 없는 role은 확인 자체 없음(에러-driven). answer/gatedDrain이 claim 직전 Allow, 429면 아이템 Retry+쿨다운 arming+pass 중단(overshoot는 in-flight 아이템 수로 유계)
- 파일: internal/worker/pacer.go(StatusGate 재작성), answer.go(게이트 루프), worker.go(gatedDrain/QuotaChecksFor), question/presolve/verify/comprehend.go(게이트 주입), teacher/config.go(QuotaProvider)·client.go(FetchQuotaURL), runner/config.go(OpencodeConfig quota 필드), cmd/distill/worker.go(stageQuotaChecks 배선), config/settings.yaml(subgate role+comprehend quota_url/quota_provider, pacing 게이트 키 주석). pacing 패키지·monolithic runner·subgate 무변경
- 검증: internal/worker(신규 게이트 20테스트 + answer 6테스트)·internal/teacher(FetchQuotaURL)·cmd/distill(배선) `go test -race` PASS, static binary build PASS. make lint/lint-schema PASS
- 통합 검증(sqlc 트랙 완료 후, 오케스트레이터 직접 실행): `go clean -testcache` 후 make test **19패키지 전부 -race PASS**(runner 포함 — 중단됐던 agent의 decide.go revert는 결과적으로 깨끗했음이 확인돼 추가 복원 불필요), make lint/lint-schema/build(38M)/sqlc-check 전부 exit 0

### sqlc 도입 (2026-07-18)
- sqlc.yaml(v2, postgresql, pgx/v5) 2블록: queue(스키마=migrations 0001/0002 그대로, out=internal/queue/sqlcgen), loader(스키마=internal/loader/schema.sql, out=internal/loader/sqlcgen). nullable timestamptz -> *time.Time override
- 드리프트 방지: loader 인라인 DDL 상수를 schema.sql로 추출해 loader.go가 `//go:embed`로 적용하고 sqlc가 같은 파일을 읽음(물리적 단일 소스, Go 사본 제거). queue/pacing은 기존 migration .sql이 이미 단일 소스. `make sqlc-check`가 generate 후 sqlcgen 디렉토리 git diff --exit-code로 stale 생성물 검출(내용 민감성 실측 확인)
- 전환: queue Enqueue/GetCursor/SaveCursor, loader CountPairs/MaxPairID/DeletePairsByTaskIDs (공개 시그니처 불변, 내부만 생성 코드로)
- 수기 유지(사유 주석): queue claim/complete(stage 컬럼 런타임 조립이라 sqlc 정적 모델 불가) + fail/retry/supersede(owner-fence transition 패밀리 응집), loader InsertRecord(frozen runner_test의 `HasPrefix(sql,"INSERT")` 매칭이 sqlc 선두 주석과 충돌 + map[string]any 시그니처 의존 — 해제 조건: runner 테스트 매칭 완화 시 전환 가능, go_type interface{} override 방식 검증됨)
- Makefile: `make sqlc`(generate)/`make sqlc-check`. 로컬 sqlc 바이너리(v1.23.0) 우선, 미설치 환경은 pin된 `go run` fallback(macOS 최신 SDK에서 pg_query_go CGO 충돌로 로컬은 바이너리 필수 — Makefile 주석)
- 테스트 정합(비-frozen 2건, 계약 불변): loader_test(CountRows 스캔 *int64, delete SQL ANY($1::text[])), flush_test(mock 스캔 *int64). go.mod 무변경
- 잔여 전환 후보: loader InsertRecord(위 해제 조건), queue fail/retry/supersede(응집 사유로 보류), pacing 쿼리(pacing .go 안정화 후 3번째 블록)
- 레이아웃 통합 -> internal/db (같은 날 후속): sqlc 자산(sqlc.yaml/스키마/쿼리/생성물)을 도구명 폴더 대신 책임 경계인 internal/db로 이동. 2블록 -> 단일 블록(양 스키마+쿼리 -> internal/db/sqlcgen 단일 패키지, 생성 시그니처·EnqueueParams 불변, distillation_pairs.created_at만 *time.Time로 바뀌나 loader 미스캔 필드라 무영향). 마이그레이션 .sql은 internal/db/migrations/로 이동(도메인 접두어 queue_0001_queue/queue_0002_fencing_retry_projected/pairs_0001_distillation_pairs), db.Migrate가 go:embed 명시적 순서 리스트(queue 0001 -> 0002 -> pairs)로 적용하고 sqlc가 같은 파일을 스키마로 읽음(go:embed는 상위 디렉토리 접근 불가 -> db가 유일한 embed 소유자, 물리적 단일 소스 강제)
- 3갈래 적용 통합: db.MigrateQueue/db.MigratePairs/db.Migrate 신설. worker openDB는 db.Migrate 단일 호출로 단순화(loader.CreateTable+queue.Migrate 2호출 제거, loader import도 제거). queue.Migrate/loader.CreateTable은 시그니처 유지 thin delegate(db.MigrateQueue/db.MigratePairs 위임 + 기존 에러 문구 보존 -> frozen 호출자 monolithic run/load/coverage/테스트 계약 불변). pacing.Migrate·pacing/migrations는 monolithic 전용이라 이동 제외(monolithic 제거 시 함께 정리). Makefile: sqlc.yaml이 internal/db 기준 상대경로라 `sqlc generate -f internal/db/sqlc.yaml`, sqlc-check diff 경로 internal/db/sqlcgen. 검증: make lint/lint-schema/test(19패키지 -race)/build/sqlc 전부 exit 0, 드리프트 장치는 쿼리 변경 시 생성물 diff 발생·원복 시 동일 실측(git diff --exit-code 게이트는 생성물 git add 후 활성)

### 스키마 단일화 + 전용 migrate job (2026-07-18, 사용자 지시)
- 버전 마이그레이션 폐기: internal/db/migrations/(queue 0001/0002 + pairs) 3파일을 **schema.sql 1개**로 병합(0002 ALTER 컬럼을 CREATE에 흡수, flush 인덱스는 projected_at 최종형, payload 컬럼도 통합). db.Migrate가 이 파일 하나를 embed·적용, sqlc도 같은 파일. backward-compat 불필요(drop+redeploy)라 버전 히스토리 없이 최종형만
- self-migrate 제거: openDB(worker/ingest/flush 공통)가 더 이상 스키마를 만들지 않음(pacing.Migrate도 제거 — worker는 pacing DB 테이블 미사용, status-check라 /quota만 읽음). 워커가 각자 CREATE하던 것/동시 CREATE race 자체가 사라짐
- 전용 `distill migrate` 명령 신설: 스키마를 **딱 1번** 적용 후 종료. compose는 migrate 서비스가 `distill migrate`(기존 ingest 오용 교정), 워커는 depends_on service_completed_successfully로 게이트. k8s는 **distillation-migrate Job**(migrate-job.yaml) 신설 + 워커/cronjob의 initContainer를 wait-for-postgres -> **wait-for-schema**(distillation_items 존재까지 대기)로 교체 — 사용자 결정 "명시적 Job이 낫다". 검증: build+19패키지 test green, kustomize base/prod PASS(Job+wait-for-schema 렌더), compose config PASS
- ingest batch를 config로: `ingestLimitPerSource` Go 상수(32) -> `ingest.limit_per_source` 설정(=1). cadence는 ofelia `@every 5m`. "5분마다 1개"=스케줄(ofelia)×배치(config), 코드 상수 아님

### compose 동시 기동 정합 (2026-07-18, subgate와 한 호스트)
- 포트: 비충돌 확인(distillation publish=5432만, subgate=8080만, 프로젝트/네트워크/볼륨명 분리)
- 실결함 발견·수정: baked `svc-subgate.subgate:8080`(base_url+quota_url)이 compose 네트워크에서 미해석 -> quota_url은 env override도 없어 게이트 fetch 실패 = 전 step 보수적 skip(생성 0)이 될 상황. 해결: subgate compose가 공유 네트워크 `subgate`+DNS alias를 만들고, LLM worker 5종(comprehend/question/presolve/answer/verify)이 external 조인 — baked 주소 그대로 동작(ingest/flush/db는 LLM 무관이라 미조인). 기동 순서: subgate 먼저
- live 검증: 공유 네트워크에서 baked 주소로 /quota curl 정상, 양쪽 compose config PASS. deployments/README 반영
- cross-repo 계약 고정: live subgate의 실제 GET /quota 응답을 testdata/quota_live_sample.json으로 캡처해 `TestParseSnapshot_LiveSubgateSample`(internal/pacing) 추가 — StatusGate가 의존하는 계약(provider 조회, null used_percent -> Known()==false, null resets_at -> zero time, pipeline 소스 버킷) 을 실캡처 기준으로 회귀 고정. X-Subgate-Source 태깅도 live 검증(verify-test 버킷에 requests/tokens 기록 확인). 전체 make test 19패키지 green 유지

### secrets 체계 + README 3분할 재작성 (2026-07-18, grilling 확정)
- gitignore 분산(사용자 지시 "최상위 몰빵 금지"): deployments/base·overlays/prod(`secret-*.env`)·overlays/docker-compose(`.env`)에 개별 .gitignore, 루트는 `*-secrets.zip` 한 줄만
- secret 정리: opencode.env -> secret-opencode.env rename(+untrack, 컨벤션 통일), postgres secret 2종이 0바이트였던 것을 발견해 값 생성·채움(랜덤 생성, 대화 기록에 값 미노출), compose .env change-me 교체
- distillation-secrets.zip 생성(aiauto/01serving 컨벤션): 레포 루트에서 unzip 시 generator 참조 경로에 4파일 배치, 루트 gitignore로 zip 자체는 git 밖. 주의: 기존 라이브 postgres PVC가 있으면 새 password와 불일치(DB 데이터 디렉토리에 이미 initdb됨) — 그 경우 구 값 유지 또는 PVC 초기화 필요
- README 재작성(grilling 수렴): main=개요/아키텍처(flow 표+role+큐 요점)/구조/배포 링크/운영(pacing status-check·큐 모니터링 SQL·export/HF), 배포 상세는 deployments/README.md 신설, 데이터(스키마 필드·4축·parquet 불변)는 back/schemas/README.md 신설. monolithic 서술 완전 제거(deprecated 서술 금지 규칙), 트리에서 runner/pacing 라인 제외
- subgate auth.json -> PVC 전환 완료(상세는 subgate todo sec 5): Secret+emptyDir의 회전 유실 결함 해소, 레포에서 auth 파일 소멸, 시딩 절차 subgate deployments/README.md sec 2-1
- postgres secret 단일화(사용자 지적 "overlay마다 사본이면 base가 왜 필요"): overlays/prod의 `behavior: replace` secretGenerator + env 사본 제거 -> base/postgres 것 하나만 사용(전 overlay 상속, kustomize가 해시명 참조 자동 재작성 — env 변경 시 워커 자동 롤링). .env.example도 삭제(zip의 .env가 주석 포함 원본, README가 키 문서화). zip은 3파일로 재생성. README 목록 동기화

### export 저장소 백엔드 중립화 + Makefile 정리 (2026-07-18, 사용자 지시)
- 사용자: "무조건 NFS냐, 그냥 PVC면 충분" -> 스토리지 백엔드 bake-in 전면 제거(일관 동기화)
  - deployments/base/pvc-data.yaml: 정적 NFS PV(server/path placeholder) 삭제, plain PVC `pvc-distillation-data`(RWO 100Gi)만 — 백엔드는 클러스터 default StorageClass 소관
  - 경로 /mnt/nfs/distillation -> `/mnt/distillation`, config 키 flush.`nas_dir` -> `export_dir`, Go 필드 flush.Config.NASDir -> ExportDir(+검증 에러 문구), exporter.DefaultOutputDir 동기화
  - cronjob(volume nas->data, claim명), compose(nas-init->data-init, distillation-nas->distillation-data 볼륨, ofelia label), README(4-2를 PVC 일반론으로 재작성, 사전조건·export 명령 경로), 전 주석의 NAS 표현 -> export volume (능력 서술인 "fcntl은 NFS cross-host에서도 동작" 류만 유지)
  - 검증: build + flush/worker/queue/runner fresh -race PASS, 전체 make test 19패키지 ok, kustomize base·prod PASS, compose config PASS
- Makefile 정리(사용자 지시): SCHEMAS/SQLC_VERSION/SQLC 변수·sqlc-check 타겟·설명 주석 전부 삭제, `sqlc` 타겟은 `sqlc generate -f internal/db/sqlc.yaml` 한 줄(로컬 sqlc 바이너리 필요). 드리프트 검출이 필요하면 CI에서 `make sqlc && git diff --exit-code`로 조합 가능
- go.work 신설(레포 루트): 모듈 루트가 back/으로 내려가 gopls가 레포 루트 기준으로 모듈을 못 찾던 가짜 진단 해결. 빌드는 back/ 내 실행이라 무영향(단일 모듈)

### 레포 재구조화: 루트 Go 자산 -> back/ (2026-07-18)
- 루트의 Go 모듈 자산(go.mod/go.sum, cmd/, internal/, config/, prompts/, schemas/, Dockerfile, Makefile, .dockerignore)을 back/ 하위로 이동해 모듈 루트를 back/로 통일. 레포 루트에는 deployments/·research/·todos/·README만 남김. make는 `make -C back` 또는 back/에서 실행
- deployments 참조 경로 갱신 완료: docker-compose build context를 `../../../back`로, prod/base kustomization·Dockerfile COPY 경로를 back/ 기준으로 수정(빌드/매니페스트가 새 모듈 루트를 가리킴)

### 사용자 결정 로그
- (2026-07-12) 데이터 저장: DB 버퍼→임계치→NAS Parquet flush→DB 삭제. 저작권 가드 미적용(학습 판별 불가). verifier 실제 실행. live 스파이크는 codex direct quota 필드명 1회 캡처(수동 운영 아님, cli_unknown pacing으로 스파이크 없이 자동 동작)
- (2026-07-17) 재설계 확정: DB queue 상태머신(2-5), flush append-only B1(2-5-6), reasoning cot/cot_raw(2-5-5), presolve 게이트, opencode comprehend, ofelia 배포. codex 과설계 판정은 기각(사용자 요구 우선), 실재 결함만 반영
- (2026-07-18) quota pacing 무상태 전환: 예약 원장 기각·삭제, subgate GET /quota 사실만 읽는 per-step 게이트 확정(sec 6-3-4)
