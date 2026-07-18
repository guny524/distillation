# Distillation Pipeline

실제 artifact(arXiv/StackExchange/k8s docs/PMC)에서 질문을 역생성하고, teacher LLM의 추론 trajectory(ko/en 2-lane)를 수집·검증해 distillation 학습 데이터셋(Parquet)을 만드는 파이프라인. PostgreSQL 큐 상태머신 위에서 7단계 worker가 병렬로 돌고, 모델 호출은 전부 OpenAI 호환 endpoint라 role별로 자유롭게 지정한다.

## 1. 아키텍처

### 1-1. 데이터 flow (DB 큐 상태머신)

PostgreSQL `distillation_items` 큐를 7단계 worker가 병렬로 소진한다. 각 worker는 "이전 단계 timestamp는 찍혔고 자기 단계는 비어 있는" 아이템을 claim한다.

| 단계 | 실행 | 하는 일 | 사용 role |
|---|---|---|---|
| ingest | 정기 | 소스 어댑터가 새 artifact 발견, 발췌와 함께 큐 등록 | 없음 |
| comprehend | 상시 | opencode CLI가 소스 링크 fetch + auto-compact + 이해 -> digest | opencode provider |
| question | 상시 | digest에서 질문 역생성(Instruction Backtranslation) + Evol 변형 | generator |
| presolve | 상시 | student가 먼저 풀고 judge가 채점, 이미 풀면 폐기(soft-delete) | student + judge |
| answer | 상시 | ko/en 2-lane teacher trajectory 생성, 반대 lane은 번역 | teacher + translator |
| verify | 상시 | 코드는 sandbox 실제 실행, 그 외는 verifier 판정 | verifier |
| flush | 정기 | presolve+verify 통과분만 `distillation_pairs`로 투영 후 Parquet 반출 | 없음 |

presolve는 반출의 필수 관문이다 — student(경량 모델)가 이미 푸는 문제는 teacher까지 가지 않고 버려진다.

### 1-2. 역할 endpoint

role(teacher/generator/student/judge/translator/verifier + comprehend의 opencode provider)은 `back/config/settings.yaml`에서 base_url로 지정하며, OpenAI 호환이면 무엇이든 된다. 역할이 다른 둘이 같은 endpoint를 써도 되고, `quota_url`이 설정된 role은 quota pacing 대상이 된다(sec 4-1). 배포 시 `SUBGATE_ENDPOINT_<ROLE>` env로 재빌드 없이 오버라이드할 수 있다 — role별 기본값과 오버라이드 방법은 `back/config/settings.yaml`·[deployments/README.md](deployments/README.md) 참조.

### 1-3. 큐 상태머신 요점

- 상태 = 단계별 nullable timestamp 컬럼(`queued_at -> comprehended_at -> questioned_at -> presolved_at -> answered_at -> verified_at -> projected_at`). soft-delete는 `superseded_at`, 실패는 `failed_at/fail_stage/fail_reason`
- claim은 `SELECT ... FOR UPDATE SKIP LOCKED` 짧은 트랜잭션으로 `lock_owner_id/lock_at`을 세팅하고 즉시 커밋. 완료/실패/payload 기록은 전부 owner fencing(`AND lock_owner_id = $owner`)이라, stale로 회수된 worker의 늦은 write는 no-op
- 일시 오류(429/5xx/네트워크/408)는 `retry_count/next_attempt_at` 백오프로 재시도하고, 스키마/계약 오류만 terminal 실패. 단계 완료 시 재시도 예산은 리셋
- 반출된 Parquet은 append-only 불변(순차 재현학습 보장). soft-delete는 반출 전 아이템에만 허용

## 2. 디렉토리 구조

```text
distillation/
- back/                            # Go 서비스 전체 (모듈 루트)
  - Dockerfile                     # alpine + Go static binary + opencode CLI
  - Makefile                       # lint, sqlc, test, build, image-build/push
  - cmd/distill/                   # CLI: 큐 worker subcommand 7종 (ingest/comprehend/question/presolve/answer/verify/flush)
  - internal/
    - db/                          # DB 연결(pgx v5) + 스키마 단일 소스(sqlc.yaml·migrations·queries.sql·sqlcgen) + db.Migrate
    - queue/                       # DB 큐 상태머신 (timestamp 상태, claim/owner fencing)
    - worker/                      # stage worker 7종 + quota StatusGate
    - ingest/                      # artifact 소스 어댑터 4종 (arXiv/StackExchange/k8s/PMC)
    - pipeline/                    # backtranslate/mutate/presolve/2-lane/verify 스테이지 로직
    - teacher/                     # 역할별 OpenAI 호환 endpoint 클라이언트 (재시도/타임아웃/소스 태그/quota fetch)
    - model/ loader/ exporter/ flush/ coverage/   # 스키마 검증, DB 적재, Parquet 반출, 분포 조회
  - config/                        # settings.yaml(역할 endpoint/pacing/worker), taxonomy.yaml, opencode.json
  - prompts/                       # teacher 프롬프트 템플릿 (distill-prompt ConfigMap 단일 소스)
  - schemas/                       # 데이터 스키마 + 4축 분류 (README 포함)
- deployments/                     # k8s kustomize + docker-compose (README 포함)
- research/                        # 리서치 문서
- todos/                           # 이슈별 작업 문서
```

## 3. 배포

배포 전 `secrets-distillation.zip`을 **레포 루트에서** 압축 해제한다. kustomize secretGenerator / compose env_file이 읽는 정확한 경로에 배치된다:

```bash
unzip secrets-distillation.zip

# unzip -l secrets-distillation.zip 으로 확인 시 아래 파일들이 포함:
deployments/base/postgres/secret-postgres.env      # POSTGRES_USER/PASSWORD/DB (postgres init + 전 worker DB 접속, 전 overlay 공용)
deployments/base/secret-opencode.env               # OPENCODE_SUBGATE_API_KEY (comprehend의 opencode provider용)
deployments/overlays/docker-compose/.env           # compose 비밀 (POSTGRES_PASSWORD 등)
```

k8s(kustomize base/overlays)·docker-compose(ofelia) 절차와 config 주입, 재압축 — [deployments/README.md](deployments/README.md)

## 4. 운영

### 4-1. quota pacing (step별 status-check)

`quota_url`이 설정된 role을 쓰는 단계는 아이템 claim 직전마다 그 endpoint의 `GET /quota`에서 사실(used_percent, resets_at)을 읽어 스스로 판단한다. 잔량의 진실은 외부 status뿐이며 DB에 소비 예정을 기록하지 않는다.

- 주간(secondary): `used% >= weekly_cap_pct x (reset 주기 경과 비율)`이면 그 step만 쉼 — 주간 budget을 시간에 비례 배분한 것과 등가라, 같은 계정의 파이프라인 밖 소비가 늘면 자동 감속하고 남으면 채워 쓴다
- 5h(primary): `used% >= primary_cap_pct - safety_margin`이면 쉼 (배분 없는 hard cap)
- quota가 unknown인 provider는 게이트를 열어두고, 429 관측 시 쿨다운 동안 쉼
- `quota_url`이 없는 role은 확인 없이 호출하고 에러(429/insufficient_quota)로만 대응

값은 `back/config/settings.yaml`의 `pacing:` 블록(weekly_cap_pct, primary_cap_pct, safety margin, 쿨다운)으로 조정한다.

### 4-2. 큐 모니터링

단계별 잔량:

```sql
SELECT count(*) FILTER (WHERE comprehended_at IS NULL)                                AS to_comprehend,
       count(*) FILTER (WHERE comprehended_at IS NOT NULL AND questioned_at IS NULL)  AS to_question,
       count(*) FILTER (WHERE questioned_at   IS NOT NULL AND presolved_at  IS NULL)  AS to_presolve,
       count(*) FILTER (WHERE presolved_at    IS NOT NULL AND answered_at   IS NULL)  AS to_answer,
       count(*) FILTER (WHERE answered_at     IS NOT NULL AND verified_at   IS NULL)  AS to_verify,
       count(*) FILTER (WHERE verified_at     IS NOT NULL AND projected_at  IS NULL)  AS to_flush
FROM distillation_items
WHERE superseded_at IS NULL AND failed_at IS NULL;
```

- 실패 원인: `SELECT fail_stage, fail_reason, count(*) FROM distillation_items WHERE failed_at IS NOT NULL GROUP BY 1,2 ORDER BY 3 DESC;`
- 백오프 대기 중: `SELECT count(*) FROM distillation_items WHERE next_attempt_at > now();`

### 4-3. export / HF push

flush CronJob이 버퍼(`distillation_pairs`)가 임계에 도달하면 Parquet 배치를 export 볼륨(`/mnt/distillation`)에 append하고 버퍼를 비운다. 수동 반출과 HuggingFace 업로드:

```bash
distill export --output-dir /mnt/distillation --shard-size 50000 --compression zstd
pipx run huggingface-cli upload YOUR_HF_REPO /mnt/distillation --repo-type dataset
```

데이터 스키마 필드(cot/cot_raw 등), 4축 분류, Parquet 불변 정책 — [back/schemas/README.md](back/schemas/README.md)
