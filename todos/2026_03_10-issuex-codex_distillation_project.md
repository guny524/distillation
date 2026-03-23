# Codex Distillation Pipeline
- 이슈 주소: `local-only`
- Codex Pro 구독 quota의 남는 한도를 최대한 소진하면서, gpt-5.4 teacher model에서 distillation용 Q&A 데이터셋을 주기적으로 생성하는 파이프라인 구축
- 참고: chat gpt 대화 https://chatgpt.com/c/69b02029-cd28-83a3-abfb-daff5d22148f

## 1. 배경(현재 이슈의 대략적인 이전 맥락)
- Pro 구독 요금제($200/mo)를 쓰는데 매번 한도를 못 채워서 돈이 아까움
- codex exec를 비대화형으로 주기적 실행하여 남는 quota로 distillation 데이터를 적재하려 함
- logprobs 없는 black-box teacher 환경이므로, prompt-response pair 기반 distillation을 사용
- 로컬 노트북이 아니라 k8s cluster 서버에서 CronJob으로 운영할 예정
- HuggingFace datasets 호환 형태(Parquet)로 최종 산출물을 만들어야 함

#### 1-1. Codex Pro 구독 제한 구조
- credit 기반 시스템, 토큰 단위 한도는 비공개
- GPT-5.4 Pro: 223~1,120 local messages / 5h 윈도우 (메시지 복잡도에 따라 변동)
- 5시간 윈도우 제한 + 주간 누적 제한 별도 존재 (커뮤니티 관찰: ~2,000~2,500 credits/week)
- 프로그래밍적 quota 조회 방법 없음 (CLI 명령, API 엔드포인트 모두 없음)
- 인터랙티브 `/status`는 세션 내에서만 동작하고 버그 다수 (신뢰성 부족)
- rate limit 도달 시에만 리셋 시간 표시, 사전 확인 불가

#### 1-2. quota 간격 계산 가이드
- Pro 기준 GPT-5.4는 5시간에 223~1,120 메시지, distillation Q&A는 응답이 길어 하한(223)에 가까울 것으로 추정
- 하루 24시간 = 4.8개 윈도우, 보수적으로 하루 ~200회가 안전 범위
- 단, 주간 제한(~2,000~2,500 credits)을 고려하면 하루 전부 소진하면 주 후반에 quota 부족
- 권장: 주간 budget을 7일로 나눠 하루 균등 배분, 그 안에서 cron 간격 설정
  - 예: 주간 2,000 credits, 1건당 ~7 credits 가정 -> 주 ~285건 -> 일 ~40건 -> 36분에 1회
  - 예: 보수적으로 1시간에 1회 -> 일 24건 -> 주 168건
- 실제 credit 소모는 응답 복잡도에 따라 변동하므로, 처음에는 보수적으로 시작하고 rate limit 에러 빈도를 보면서 조절
- rate limit 에러 발생 시 해당 CronJob 실행은 skip하고 다음 주기까지 대기
- 설정 파일(`config/settings.yaml`)에서 cron 간격을 조절할 수 있게 문서화

#### 1-3. HuggingFace 데이터셋 호환 요구사항
- HF 권장 포맷: Parquet (100GB+ 대규모), JSONL (개발 단계)
- Parquet 자체가 columnar 압축 내장, 추가 gzip/zstd 지원
- 샤딩: `train-00000-of-00010.parquet` 패턴, 파일당 ~5-10GB 권장
- HF 업로드 제한: 파일당 50GB
- 필수 메타데이터: README.md YAML frontmatter에 `configs`/`data_files`/`split` 정의
- `datasets.load_dataset()`으로 자동 인식 가능한 디렉토리 구조 필요
- 실무 파이프라인: JSONL로 수집 -> Parquet 변환 -> HF push

#### 1-4. 참고해야 할 기존 프로젝트 배포 패턴
- aiauto deployments: `/Users/min-jo/go/src/gitlab.com/01ai/eng/aiauto/aiauto/deployments/`
  - base/overlays/components 구조, PostgreSQL Deployment+Service+PVC+secretGenerator
  - Dockerfile 패턴: `journalGrpcStorageProxy` (python-alpine), `dashboard` (multi-stage)
- 01serving deployments: `/Users/min-jo/go/src/gitlab.com/01ai/eng/01serving/01serving/deployments/`
  - base/overlays 구조, PVC, secretGenerator(huggingface-token.env)
- codex CLI: npm 기반 설치, Node.js 이미지에서 Dockerfile 빌드

---

## 2. 요구사항(구현하고자 하는 필요한 기능)
### 2-1. codex exec 기반 Q&A 데이터 자동 생성
- k8s CronJob이 설정된 간격으로 codex exec 실행
- codex exec 1회 호출 = Q&A 데이터 1건 생성
- cron 간격은 사용자가 설정 파일에서 조절 (quota 소진 속도에 따라)
- rate limit 에러 감지 시 해당 실행 skip

### 2-2. 고정 프롬프트 기반 자율 실행
- codex agent가 고정 프롬프트(`prompts/distillation.md`)를 읽고 자율적으로 실행
- 프롬프트가 codex에게 지시하는 내용:
  1. coverage.json 파일을 읽어서 현재 부족한 도메인/능력/난이도/형태를 파악
  2. 부족한 축 조합에 대해 Q&A 1건 생성
  3. 결과를 지정된 경로에 JSONL로 저장
- Python orchestrator(collector) 같은 별도 프로그램 불필요, codex가 agent로서 자율 수행

### 2-3. 4축 분류 체계 기반 데이터 다양성 확보
- domain(분야) x capability(능력) x difficulty(난이도) x task_shape(출력 형태) 조합으로 다양성 관리
- init container가 PostgreSQL에서 기존 데이터의 축별 분포를 쿼리하여 coverage.json 생성
- codex는 coverage.json을 읽고 부족한 조합을 우선 생성

#### 2-3-1. 도메인 분류 체계 (4축)
##### 축 1: domain (분야) - 13개
- `software-engineering`: 코드 작성, 디버깅, 아키텍처, DevOps
- `data-science`: 통계, ML, 데이터 분석, 시각화
- `mathematics`: 수학 문제, 증명, 수치 계산
- `natural-science`: 물리, 화학, 생물, 지구과학
- `finance`: 주식, 투자, 금융 상품, 재무 분석, 퀀트, 리스크 관리
- `business`: 전략, 마케팅, 경영 분석, 운영
- `legal-compliance`: 법률 해석, 규정 준수, 계약 검토
- `education`: 교육 자료 작성, 커리큘럼 설계, 평가
- `creative-writing`: 소설, 시, 카피라이팅, 시나리오
- `technical-writing`: 문서화, API 문서, 매뉴얼, 보고서
- `linguistics`: 번역, 문법, 언어 분석, 다국어
- `philosophy-ethics`: 윤리적 판단, 논증, 사고 실험
- `general-knowledge`: 상식, 역사, 문화, 시사

##### 축 2: capability (요구 능력) - 8개
- `reasoning`: 논리적 추론, 인과 분석
- `knowledge-recall`: 사실 지식 인출
- `generation`: 새로운 콘텐츠 생성
- `transformation`: 입력을 다른 형태로 변환 (요약, 번역, 포맷 변경)
- `evaluation`: 품질 판단, 비교 분석, 비평
- `planning`: 계획 수립, 단계 분해, 전략
- `problem-solving`: 제약 조건 하 해결책 도출
- `instruction-following`: 복잡한 지시 정확히 수행

##### 축 3: difficulty (난이도) - 3개
- `easy`: 단일 단계, 명확한 정답
- `medium`: 2-3단계, 약간의 판단 필요
- `hard`: 다단계, 모호성 존재, 전문 지식 필요

##### 축 4: task_shape (출력 형태) - 6개
- `short-text`: 1-3문장 응답
- `long-text`: 여러 단락 구조화된 텍스트
- `code`: 프로그래밍 코드
- `structured-data`: JSON, 표, 리스트 등 구조화된 데이터
- `analysis-report`: 분석 + 결론 + 근거가 있는 보고서
- `step-by-step`: 순서가 있는 절차/가이드

### 2-4. 데이터 저장 및 배포 파이프라인
- codex가 JSONL 출력 -> PostgreSQL 적재 -> Parquet 변환 -> NFS(PV) 백업 + HF push
- PostgreSQL: k8s cluster 내 Deployment (aiauto 패턴 참고)
- NFS: PV로 마운트, kustomization에서 서버 정보 수정 가능하게
- HuggingFace 호환 Parquet 포맷으로 최종 산출물 생성

### 2-5. k8s CronJob 배포
- namespace: `distillation`
- base/overlays/components 구조 (aiauto/01serving 패턴)
- Dockerfile: node 이미지 + npm install codex CLI
- codex 인증: Secret으로 토큰 mount
- init container: python-alpine + psycopg2, DB에서 coverage.json dump
- `--full-auto` 또는 `--sandbox workspace-write` 플래그로 codex 자율 실행

#### 2-5-1. CronJob pod 실행 흐름
```
[k8s CronJob - 사용자 설정 간격]
  -> init container (python-alpine + psycopg2)
     : PostgreSQL에서 4축 domain 분포 쿼리 -> /workspace/coverage.json 저장
  -> main container (node + codex CLI)
     : codex exec --full-auto < /prompts/distillation.md
     : 프롬프트가 codex에게 지시:
       1. /workspace/coverage.json 읽어서 부족한 분야 파악
       2. 부족한 축 조합에 대해 Q&A 1건 생성
       3. /workspace/output/result.jsonl 에 저장
  -> post-process (entrypoint script 후반부 또는 별도 CronJob)
     : JSONL -> PostgreSQL 적재 + Parquet 변환 -> NFS 백업
  -> pod 종료
```

#### 2-5-2. 프로젝트 디렉토리 구조
```
distillation/
- Dockerfile
- Makefile
- README.md
- config/
  - taxonomy.yaml               # 4축 도메인 분류 체계
  - settings.yaml               # cron 간격 계산 가이드, DB 접속 등
- prompts/
  - distillation.md             # codex가 읽는 고정 프롬프트
- schemas/
  - distillation.schema.json    # codex 출력 JSON schema
- scripts/
  - dump_coverage.py            # init container: DB -> coverage.json
  - load_to_db.py               # post: JSONL -> PostgreSQL
  - export_parquet.py           # post: PostgreSQL -> Parquet
- deployments/
  - base/
    - kustomization.yaml
    - cronjob.yaml
    - postgres/
      - deployment.yaml
      - service.yaml
      - pvc.yaml
      - kustomization.yaml
    - pvc-data.yaml             # NFS PV/PVC (사용자가 서버 정보 수정)
  - overlays/
    - prod/
      - kustomization.yaml
      - namespaces.yaml
- docs/
  - 2026_03_10-issuex-codex_distillation_project.md
```

---

# AI 결과

## 3. (AI가 확인한) 기존 코드/구현의 핵심내용들/의도들
- 기존에 codex가 작성한 `collector.py`가 있었으나, 사용자의 의도와 맞지 않아 폐기 결정
  - collector.py: Python orchestrator가 프롬프트를 검색/파싱하고, codex exec를 subprocess로 호출하고, 결과를 JSONL로 적재하는 구조
  - 문제: codex agent가 자율적으로 파일 I/O하는 구조가 아니라, Python이 모든 것을 관리하는 구조
- 기존 스키마(`distillation-batch.schema.json`): 6개 고정 배치 구조 -> 폐기, 1회 1건으로 변경
- 기존 프롬프트(`prompts/distillation.md`): "6개 샘플 배치를 만들어라" -> 폐기, 커버리지 기반 단일 Q&A 생성으로 변경
- 로컬 cron 예시만 문서화하는 범위 -> k8s CronJob 서버 배포로 확장

---

## 4. 생각한 수정 방안들 (ai 가 생각하기에) 구현에 필요한 핵심 변경점
### 4-1. 기존 collector.py 기반 확장 (폐기)
- collector.py의 frontmatter 파싱, subprocess 호출 구조를 유지하면서 quota 관리/다양성 로직 추가
- 문제: Python orchestrator가 프롬프트를 동적 생성하는 구조이고, codex agent 자율 실행이 아님

### 4-2. codex agent 자율 실행 + 고정 프롬프트 (채택)
- 고정 프롬프트가 codex에게 파일 읽기/스크립트 실행/데이터 생성을 지시
- init container가 DB 분포를 coverage.json으로 dump
- codex는 coverage.json만 읽고 부족한 분야 Q&A 생성
- 장점: codex가 agent로서 자율 판단, Python 의존성 최소화

### 4-3. 별도 스케줄러 Deployment (미채택)
- 항상 떠 있는 Deployment + 내부 스케줄러로 자체 스케줄링
- 문제: CronJob이 더 단순하고 k8s 네이티브, 불필요한 복잡성

---

## 5. 최종 결정된 수정 방안
- 4-2 채택: codex agent 자율 실행 + 고정 프롬프트 + k8s CronJob + PostgreSQL + Parquet + NFS
- 기존 코드(collector.py, batch schema, batch prompt)는 전부 폐기하고 처음부터 새로 설계

### 5-1. 최종 결정 이유 1: codex agent 자율 실행
- 프롬프트가 codex에게 "파일 읽기 -> 부족한 분야 파악 -> Q&A 생성 -> 파일 쓰기"를 지시하는 것이 Python orchestrator보다 단순하고 codex의 agent 능력을 활용
- coverage.json은 init container가 미리 생성해두므로 codex 실행 환경에 DB 클라이언트 불필요

### 5-2. 최종 결정 이유 2: k8s CronJob 서버 배포
- 로컬 노트북이 아니라 서버에서 안정적으로 운영해야 하므로 k8s CronJob이 적합
- aiauto/01serving 기존 배포 패턴(base/overlays/components)을 그대로 따름
- Dockerfile은 node 이미지 + npm install codex CLI로 별도 빌드 (aiauto journalGrpcStorageProxy 패턴)

### 5-3. 최종 결정 이유 3: PostgreSQL + Parquet + NFS + HF push
- PostgreSQL: 분포 쿼리(gap filling)에 SQL이 가장 적합, k8s cluster에 이미 배포 패턴 있음
- Parquet: HuggingFace 데이터셋 표준 포맷, columnar 압축 효율적
- NFS: PV로 마운트하여 백업, 기존 인프라 활용
- JSONL -> PostgreSQL -> Parquet 파이프라인이 수집/분석/배포 각 단계에 최적

### 5-4. 최종 결정 이유 4: cron 간격 사용자 설정
- Codex Pro quota를 프로그래밍적으로 조회할 방법이 없으므로 자동 조절 불가
- 사용자가 설정 파일에서 간격을 수동 설정하고, 문서에 계산 가이드를 제공
- rate limit 에러 시 해당 실행 skip으로 안전장치

---

## 6. 코드 수정 요약
- 기존 코드 전부 폐기하고 새로 작성
### 6-1. 프로젝트 기반 구조
- [x] `Dockerfile`: node:22-slim 기반, python3+psycopg2-binary 설치, @openai/codex npm install, non-root user(codexuser:distill), /workspace에 prompts/config/schemas/scripts COPY, entrypoint.sh ENTRYPOINT
  - `entrypoint.sh` 신규 작성: codex exec --full-auto 실행 후 result.jsonl 존재 시 load_to_db.py 호출하여 PostgreSQL 적재
- [x] `Makefile`: lint(scripts/*.py py_compile), lint-schema(schemas/*.json JSON 유효성), build(docker build -t distillation .)으로 재구성. 기존 collector.py 대상 제거
- [x] `README.md`: 새 아키텍처 기반으로 전면 재작성
  - 아키텍처 흐름도, 디렉토리 구조, 사전조건, 배포(Secret/NFS/이미지), cron 간격 계산 가이드, 4축 분류 체계, Parquet/HF push, 로컬 개발, 폐기 파일 목록
- [x] `.gitignore`: __pycache__/, *.pyc, data/, .ropeproject/, *.egg-info/, .env 추가

### 6-2. 설정 및 분류 체계
- [x] `config/taxonomy.yaml`: 4축 도메인 분류 체계 정의 (domain 13 x capability 8 x difficulty 3 x task_shape 6)
  - 4축(domain/capability/difficulty/task_shape)을 각각 description 포함 YAML로 정의, codex agent가 읽어서 축 의미를 이해 가능
- [x] `config/settings.yaml`: cron 간격, DB 접속 정보, NFS 경로 등 설정
  - cron schedule(기본 매시 정각), DB 접속(환경변수 참조), codex exec 설정(model/sandbox/prompt_path), output 경로, NFS backup 경로, parquet shard 설정 포함, quota 계산 가이드 주석 포함
- [x] `schemas/distillation.schema.json`: 단일 Q&A pair JSON schema (plan, reasoning_summary, self_check, quality_notes 포함)
  - 기존 batch schema(distillation-batch.schema.json) 대체, batch wrapper 제거하고 단일 Q&A pair 수준으로 평탄화, domain/difficulty/task_shape/capability_tags에 taxonomy enum 값을 명시하여 schema validation 가능

### 6-3. 프롬프트
- [x] `prompts/distillation.md`: codex agent 자율 실행용 고정 프롬프트 (coverage.json 읽기 -> 부족 분야 Q&A 생성 -> JSONL 쓰기)
  - frontmatter 제거 (collector.py용이었으므로 불필요), `codex exec --full-auto` stdin 전달용 instruction set 형태로 작성
  - 5단계 순차 지시: (1) taxonomy.yaml 읽기 -> 4축 이해, (2) coverage.json 읽기 -> 부족 조합 파악 (빈 파일/초기 상태 대응 포함), (3) distillation.schema.json 읽기 -> 출력 구조 이해, (4) 부족 축 조합 Q&A 1건 생성, (5) /workspace/output/result.jsonl에 JSON Lines 1줄 저장
  - 품질 지시: task 설계(현실적 과제, 다양성), answer 품질(plan/reasoning_summary/self_check 필수), metadata 품질(success_criteria/quality_notes), 금지사항(외부 시스템 의존, 평가 기준 없는 과제, reasoning 없는 장문 답변)
  - 저장 전 final checklist: 13개 필수 필드 존재, enum 값 taxonomy 일치, 배열 비어있지 않음, compact JSON 1줄 검증

### 6-4. 스크립트
- [x] `scripts/dump_coverage.py`: init container용, PostgreSQL에서 4축 분포 쿼리 -> coverage.json 출력
  - Where: `scripts/dump_coverage.py` 전체 신규 작성
  - What: distillation_pairs 테이블에서 domain/difficulty/task_shape 단축 GROUP BY + capability_tags unnest GROUP BY + domain x difficulty 교차 분포를 쿼리하여 coverage.json 출력
  - How: 환경변수(POSTGRES_HOST 등)로 DB 접속 -> table_exists 확인 -> 테이블 없으면 빈 coverage dict 출력 -> 있으면 6개 쿼리(total, 4축 개별, 교차) 실행 -> JSON dump -> --output 경로에 저장
- [x] `scripts/load_to_db.py`: post-process용, JSONL 파싱 -> PostgreSQL INSERT
  - Where: `scripts/load_to_db.py` 전체 신규 작성
  - What: JSONL 파일을 읽어 CREATE TABLE IF NOT EXISTS 실행 후 각 줄을 INSERT, task_id UNIQUE 충돌 시 ON CONFLICT DO NOTHING으로 skip
  - How: positional args로 여러 JSONL 파일 수용, --schema 옵션으로 jsonschema validation(패키지 없으면 skip), record_to_params()로 JSON -> INSERT 파라미터 매핑, 성공/skip/실패 건수 stdout 출력
- [x] `scripts/export_parquet.py`: PostgreSQL -> Parquet 변환 + NFS 백업
  - Where: `scripts/export_parquet.py` 전체 신규 작성
  - What: distillation_pairs 전체를 SELECT하여 pyarrow Table로 변환 후 HuggingFace datasets 호환 Parquet 샤드로 출력
  - How: 명시적 pa.schema 정의(capability_tags/success_criteria/plan/self_check/quality_notes -> list<string>, created_at -> timestamp(us,UTC)), --shard-size 기준으로 train-00000-of-NNNNN.parquet 패턴 파일 생성, references_ 컬럼명을 Parquet에서 references로 매핑

### 6-5. k8s 배포
- [x] `deployments/base/kustomization.yaml`: base kustomize 구성
  - namespace: distillation, resources: postgres(하위 kustomization) + cronjob.yaml + pvc-data.yaml
- [x] `deployments/base/cronjob.yaml`: CronJob manifest (init container + main container)
  - schedule "0 * * * *", concurrencyPolicy Forbid, successfulJobsHistoryLimit 3, failedJobsHistoryLimit 5
  - init container: main image(distillation:latest) 재사용하여 dump_coverage.py 실행, secretRef(secret-postgres) + POSTGRES_HOST=svc-postgres
  - main container: distillation:latest, entrypoint.sh 실행, secret-postgres + secret-codex(CODEX_TOKEN), workspace/output + nfs volume mount
  - volumes: workspace-volume(emptyDir), output-volume(emptyDir), nfs-volume(pvc-distillation-nfs)
  - resources: requests cpu 200m/memory 256Mi, limits cpu 2/memory 1Gi
- [x] `deployments/base/postgres/`: PostgreSQL Deployment + Service + PVC + secretGenerator (aiauto 패턴)
  - deployment.yaml: postgres:15-alpine, port 5432, envFrom secretRef(secret-postgres), TZ/PGTZ Asia/Seoul, pvc-postgres mount, resources requests 256Mi/250m limits 2Gi/2
  - service.yaml: ClusterIP svc-postgres, port 5432
  - pvc.yaml: pvc-postgres 5Gi ReadWriteOnce
  - kustomization.yaml: resources + secretGenerator(secret-postgres.env)
  - secret-postgres.env: empty file (사용자가 POSTGRES_USER/POSTGRES_PASSWORD/POSTGRES_DB 채움)
- [x] `deployments/base/pvc-data.yaml`: NFS PV/PVC (사용자가 서버 정보 수정 가능하게 문서화)
  - PV: pv-distillation-nfs, 100Gi, ReadWriteMany, nfs.server/path에 TODO 주석, Retain reclaim policy
  - PVC: pvc-distillation-nfs, 100Gi, ReadWriteMany, volumeName으로 PV 바인딩
- [x] `deployments/overlays/prod/kustomization.yaml`: prod overlay (namespace, image tag, cron 간격)
  - resources: namespaces.yaml + ../../base, images: distillation newTag placeholder, secretGenerator: secret-postgres behavior replace
- [x] `deployments/overlays/prod/namespaces.yaml`: distillation namespace 정의
  - Namespace kind, metadata.name: distillation
- [x] `.gitignore`에 `**/secret-postgres.env` 추가

---

## 7. 문제 해결에 참고
- command: `codex exec --help`
- 참고 대화: https://chatgpt.com/c/69b02029-cd28-83a3-abfb-daff5d22148f
- Codex Pricing: https://developers.openai.com/codex/pricing/
- Codex Models: https://developers.openai.com/codex/models/
- HuggingFace Datasets Loading: https://huggingface.co/docs/datasets/en/loading
- HuggingFace Repository Structure: https://huggingface.co/docs/datasets/en/repository_structure
- aiauto deployments: `/Users/min-jo/go/src/gitlab.com/01ai/eng/aiauto/aiauto/deployments/`
- 01serving deployments: `/Users/min-jo/go/src/gitlab.com/01ai/eng/01serving/01serving/deployments/`
- aiauto Dockerfile: `/Users/min-jo/go/src/gitlab.com/01ai/eng/aiauto/aiauto/journalGrpcStorageProxy/Dockerfile`
