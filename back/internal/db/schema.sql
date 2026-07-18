-- 파이프라인 영속 계층의 단일 스키마 원본.
--
-- SINGLE SOURCE OF TRUTH: 이 파일 하나를 db.Migrate가 go:embed로 적용하고, sqlc가
-- 같은 파일을 스키마 입력으로 읽는다(internal/db/sqlc.yaml). 워커가 적용하는 DDL과
-- sqlc가 쿼리를 검증하는 DDL이 물리적으로 같아 드리프트가 불가능하다.
--
-- 버전 마이그레이션은 두지 않는다: 배포는 drop+redeploy이고 스키마 진화는 이 파일을
-- 고쳐 최종형으로 유지한다(중간 ALTER 히스토리 없음). 여러 워커가 동시에 기동해도
-- 안전하도록 CREATE ... IF NOT EXISTS 만 유지한다(idempotency, 마이그레이션 아님).
--
-- 담는 것: 큐 상태머신(distillation_items) + 소스 커서(distillation_source_cursors)
-- + 반출 버퍼(distillation_pairs). pacing 테이블(internal/pacing)과 stage payload
-- 컬럼(internal/worker)은 각 소유자가 적용하며 여기 포함하지 않는다.

-- ── 큐 상태머신 (todos sec 2-5) ──────────────────────────────────────────────
-- 소스 문서 1건 = 작업 아이템 1행. 상태는 "어느 nullable timestamp까지 채워졌는가"로
-- 표현한다(2-5-1). distillation_pairs와 분리: 큐 아이템은 question/answer 전까지
-- 질문/답변/추론이 없어 pairs의 NOT NULL 계약을 만족할 수 없다.
CREATE TABLE IF NOT EXISTS distillation_items (
    id              BIGSERIAL PRIMARY KEY,

    -- 소스 provenance (ingest 시점에 확정). source_excerpt는 짧은 발췌로,
    -- comprehend가 title/URL만이 아니라 실제 본문 근거로 digest를 만들게 한다(2-5-4).
    source_type     TEXT NOT NULL,
    doc_id          TEXT NOT NULL,
    url             TEXT NOT NULL DEFAULT '',
    license         TEXT NOT NULL DEFAULT '',
    title           TEXT NOT NULL DEFAULT '',
    locator         TEXT NOT NULL DEFAULT '',
    source_excerpt  TEXT NOT NULL DEFAULT '',
    retrieved_at    TIMESTAMPTZ,

    -- 상태머신(2-5-1): 각 단계가 순서대로 자기 timestamp를 채운다. projected_at은
    -- 반출 버퍼 투영 마커이자 아이템의 terminal 마커(2-5-6). parquet 실반출 여부는
    -- 버퍼->export 사이클이 batch 디렉토리로 관리하며 아이템별 컬럼이 아니다.
    queued_at       TIMESTAMPTZ,
    comprehended_at TIMESTAMPTZ,
    questioned_at   TIMESTAMPTZ,
    presolved_at    TIMESTAMPTZ,
    answered_at     TIMESTAMPTZ,
    verified_at     TIMESTAMPTZ,
    projected_at    TIMESTAMPTZ,

    -- Terminal 마커.
    superseded_at   TIMESTAMPTZ,            -- soft-delete, 반출 전 아이템만(2-5-6)
    failed_at       TIMESTAMPTZ,
    fail_stage      TEXT NOT NULL DEFAULT '',
    fail_reason     TEXT NOT NULL DEFAULT '',

    -- 일시 오류 재시도(2-5-3 hardening): 네트워크/429/5xx/timeout은 재예약
    -- (retry_count++, next_attempt_at=now()+backoff, lock 해제)하고 terminal 아님.
    -- claim은 next_attempt_at 경과분만 고려. last_error는 관측용.
    retry_count     INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ,
    last_error      TEXT NOT NULL DEFAULT '',

    -- 단일 재사용 락(2-5-2): (lock_owner_id, lock_at) 한 쌍, 단계별 아님. stale
    -- lock_at이면 다른 워커가 회수. 모든 write는 lock_owner_id로 fencing.
    lock_owner_id   TEXT,
    lock_at         TIMESTAMPTZ,

    -- 단계별 payload(2-5-3): 각 워커가 다음 단계가 읽을 산출물을 여기 저장.
    --   comprehend -> comprehension_digest, question -> question,
    --   presolve -> presolve_verdict, answer -> answer_payload, verify -> verification.
    comprehension_digest TEXT,
    question             JSONB,
    presolve_verdict     JSONB,
    answer_payload        JSONB,
    verification         JSONB,

    -- Idempotency(2-5-7): 같은 소스 문서는 한 번만 등록.
    UNIQUE (source_type, doc_id)
);

-- 단계별 "pending" partial index: claim 술어의 STABLE 부분(이전 단계 non-null AND
-- 이 단계 null AND 미supersede AND 미fail)을 커버해 FOR UPDATE SKIP LOCKED 후보
-- 스캔이 index-only가 되게 한다. 휘발성 락 술어(lock_at < now()-stale)는 인덱스
-- 프로브 후 평가.
CREATE INDEX IF NOT EXISTS idx_items_comprehend_pending ON distillation_items (id)
    WHERE queued_at IS NOT NULL AND comprehended_at IS NULL
      AND superseded_at IS NULL AND failed_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_items_question_pending ON distillation_items (id)
    WHERE comprehended_at IS NOT NULL AND questioned_at IS NULL
      AND superseded_at IS NULL AND failed_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_items_presolve_pending ON distillation_items (id)
    WHERE questioned_at IS NOT NULL AND presolved_at IS NULL
      AND superseded_at IS NULL AND failed_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_items_answer_pending ON distillation_items (id)
    WHERE presolved_at IS NOT NULL AND answered_at IS NULL
      AND superseded_at IS NULL AND failed_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_items_verify_pending ON distillation_items (id)
    WHERE answered_at IS NOT NULL AND verified_at IS NULL
      AND superseded_at IS NULL AND failed_at IS NULL;
-- Flush 게이트(2-5-6): presolved AND verified AND 미투영 AND 미supersede AND 미fail.
-- projected_at IS NULL 게이팅이 재시작 후 재투영(parquet 중복)을 막는다.
CREATE INDEX IF NOT EXISTS idx_items_flush_pending ON distillation_items (id)
    WHERE presolved_at IS NOT NULL AND verified_at IS NOT NULL AND projected_at IS NULL
      AND superseded_at IS NULL AND failed_at IS NULL;

-- 소스별 ingest 커서/watermark(2-5-7): 마지막 수집 위치를 기억해 재실행이 앞부터
-- 다시 수집하지 않게 한다. position은 opaque(소스별 해석: arXiv start offset 등).
CREATE TABLE IF NOT EXISTS distillation_source_cursors (
    source_type TEXT PRIMARY KEY,
    position    TEXT NOT NULL DEFAULT '',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── 반출 버퍼 (JSONL -> PostgreSQL sink) ─────────────────────────────────────
-- 검증 통과분이 여기 쌓였다가 flush가 parquet 배치로 반출 후 비운다. 앞 15개는
-- 원래 taxonomy 스키마, 이후는 artifact/reasoning 캡처 컬럼(nullable).
CREATE TABLE IF NOT EXISTS distillation_pairs (
    id              SERIAL PRIMARY KEY,
    task_id         TEXT UNIQUE NOT NULL,
    domain          TEXT NOT NULL,
    difficulty      TEXT NOT NULL,
    task_shape      TEXT NOT NULL,
    capability_tags TEXT[] NOT NULL,
    user_request    TEXT NOT NULL,
    context         TEXT NOT NULL,
    success_criteria TEXT[] NOT NULL,
    plan            TEXT[] NOT NULL,
    reasoning_summary TEXT NOT NULL,
    final_answer    TEXT NOT NULL,
    self_check      TEXT[] NOT NULL,
    quality_notes   TEXT[] NOT NULL,
    references_     TEXT[],
    artifacts       TEXT[],
    source_lane            TEXT,
    pair_id                TEXT,
    artifact_refs          JSONB,
    student_filter_verdict JSONB,
    verification           JSONB,
    difficulty_mutations   TEXT[],
    teacher_model          TEXT,
    teacher_provider       TEXT,
    cot                    TEXT,
    cot_raw                TEXT,
    has_raw_cot            BOOLEAN,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
