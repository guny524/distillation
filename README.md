# Codex Distillation Pipeline

Codex Pro 구독의 남는 한도를 최대한 소진하면서, GPT-5.4 teacher model에서 distillation용 Q&A 데이터셋을 주기적으로 생성하는 파이프라인.

## 1. 아키텍처

```text
[k8s CronJob - 사용자 설정 간격]
  -> init container: DB에서 4축 분포 쿼리 -> coverage.json
  -> main container: codex exec < prompts/distillation.md
     codex가 coverage.json 읽고 부족한 분야 Q&A 1건 생성 -> result.jsonl
     entrypoint.sh가 result.jsonl을 PostgreSQL에 적재
  -> pod 종료

[별도 실행] export_parquet.py: PostgreSQL -> Parquet -> NFS 백업 -> HF push
```

- codex exec 1회 = Q&A 1건 생성, cron 빈도로 총량 조절
- 고정 프롬프트가 codex에게 파일 읽기/데이터 생성/파일 쓰기를 지시
- 4축 분류 체계(domain x capability x difficulty x task_shape)로 데이터 다양성 관리

## 2. 디렉토리 구조

```text
distillation/
- Dockerfile                     # node:22-slim + codex CLI + python3
- entrypoint.sh                  # codex exec 실행 + DB 적재
- Makefile                       # lint, lint-schema, build
- config/
  - taxonomy.yaml                # 4축 분류 체계 정의
  - settings.yaml                # cron 간격, DB, NFS 설정
- prompts/
  - distillation.md              # codex가 읽는 고정 프롬프트
- schemas/
  - distillation.schema.json     # Q&A pair JSON schema
- scripts/
  - dump_coverage.py             # init: DB -> coverage.json
  - load_to_db.py                # post: JSONL -> PostgreSQL
  - export_parquet.py            # PostgreSQL -> Parquet + NFS
- deployments/
  - base/
    - kustomization.yaml
    - cronjob.yaml
    - pvc-data.yaml              # NFS PV/PVC
    - postgres/                  # PostgreSQL Deployment
  - overlays/
    - prod/
      - kustomization.yaml
      - namespaces.yaml
```

## 3. 사전 조건

- k8s cluster
- PostgreSQL (deployments/base/postgres/ 로 함께 배포)
- NFS 서버 (백업용, `deployments/base/pvc-data.yaml`에서 서버 정보 수정)
- Codex Pro 구독 + 인증 토큰

## 4. 배포

### 4-1. Secret 설정

```bash
# PostgreSQL 인증 정보
cat > deployments/base/postgres/secret-postgres.env <<EOF
POSTGRES_USER=distillation
POSTGRES_PASSWORD=YOUR_PASSWORD
POSTGRES_DB=distillation
EOF

# prod overlay도 동일하게
cp deployments/base/postgres/secret-postgres.env deployments/overlays/prod/secret-postgres.env

# Codex 인증 토큰 (별도 Secret 생성)
kubectl -n distillation create secret generic secret-codex \
  --from-literal=CODEX_TOKEN=YOUR_CODEX_TOKEN
```

### 4-2. NFS 설정

`deployments/base/pvc-data.yaml`에서 NFS 서버 정보 수정:

```yaml
nfs:
  server: "YOUR_NFS_SERVER_IP"   # NFS 서버 IP로 변경
  path: "/YOUR/NFS/PATH"         # NFS export path로 변경
```

### 4-3. 이미지 빌드 및 배포

```bash
# 이미지 빌드 (template.mk 기반, ghcr.io/guny524/distillation/distillation:TAG)
make image-build

# 이미지 push
make image-push

# prod overlay에서 이미지 태그 수정
# deployments/overlays/prod/kustomization.yaml의 images 섹션에서 newTag 변경

# 배포
kubectl apply -k deployments/overlays/prod/
```

## 5. cron 간격 설정 가이드

Codex Pro 구독은 credit 기반이며, 프로그래밍적으로 남은 quota를 조회할 수 없다.

### 5-1. quota 계산

- GPT-5.4 Pro: 223~1,120 messages / 5h 윈도우 (복잡도에 따라 변동)
- distillation Q&A는 응답이 길어 하한(~223) 근처
- 하루 24h = 4.8 윈도우 -> 보수적으로 하루 ~200회
- 주간 제한: ~2,000~2,500 credits, 1건당 ~7 credits
  - 주 ~285건 -> 일 ~40건 -> 36분에 1회
  - 보수적: 1시간에 1회 -> 일 24건 -> 주 168건

### 5-2. 간격 변경

`deployments/base/cronjob.yaml`의 `spec.schedule` 수정 또는 prod overlay에서 patch:

```yaml
# 30분마다
schedule: "*/30 * * * *"

# 2시간마다
schedule: "0 */2 * * *"
```

### 5-3. rate limit 에러 시

codex exec가 rate limit으로 실패하면 해당 CronJob pod는 비정상 종료. `failedJobsHistoryLimit: 5`로 설정되어 있으므로 로그 확인 가능. 다음 주기까지 자동 대기.

## 6. 4축 분류 체계

`config/taxonomy.yaml`에 정의. codex가 매 실행마다 이 파일을 읽고 분류 체계를 이해.

- domain (13): software-engineering, data-science, mathematics, natural-science, finance, business, legal-compliance, education, creative-writing, technical-writing, linguistics, philosophy-ethics, general-knowledge
- capability (8): reasoning, knowledge-recall, generation, transformation, evaluation, planning, problem-solving, instruction-following
- difficulty (3): easy, medium, hard
- task_shape (6): short-text, long-text, code, structured-data, analysis-report, step-by-step

init container가 매 실행마다 PostgreSQL에서 축별 분포를 쿼리하여 `coverage.json`을 생성. codex는 이를 읽고 부족한 조합을 우선 생성.

## 7. Parquet 변환 및 HF push

```bash
# PostgreSQL -> Parquet (NFS 경로에 저장)
python3 scripts/export_parquet.py --output-dir /mnt/nfs/distillation --shard-size 50000

# HuggingFace datasets push
pip install huggingface_hub
huggingface-cli upload YOUR_HF_REPO /mnt/nfs/distillation --repo-type dataset
```

출력 Parquet은 `train-00000-of-NNNNN.parquet` 패턴으로 HuggingFace datasets 호환.

## 8. 로컬 개발/테스트

```bash
# lint
make lint

# schema 검증
make lint-schema

# Docker 이미지 빌드
make build

# 스크립트 단독 테스트 (DB 필요)
export POSTGRES_HOST=localhost POSTGRES_PORT=5432 POSTGRES_DB=distillation \
       POSTGRES_USER=distillation POSTGRES_PASSWORD=test

python3 scripts/dump_coverage.py --output coverage.json
python3 scripts/load_to_db.py path/to/result.jsonl
python3 scripts/export_parquet.py --output-dir ./parquet_output
```

## 9. 폐기된 파일

아래 파일은 이전 구조(collector.py 기반)에서 사용하던 것으로 폐기 대상:

- `collector.py`: Python orchestrator -> codex agent 자율 실행으로 대체
- `schemas/distillation-batch.schema.json`: 6개 배치 schema -> 단일 Q&A pair schema로 대체
