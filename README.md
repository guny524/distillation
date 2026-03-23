# Codex Distillation Pipeline

Codex Pro 구독의 남는 한도를 최대한 소진하면서, GPT-5.4 teacher model에서 distillation용 Q&A 데이터셋을 주기적으로 생성하는 파이프라인.

## 1. 아키텍처

```text
[k8s CronJob - 사용자 설정 간격]
  -> init container: DB에서 4축 분포 쿼리 -> coverage.json (테이블 미존재 시 빈 coverage 생성)
  -> main container: codex exec < prompts/distillation.md
     codex가 coverage.json 읽고 부족한 분야 Q&A 1건 생성 -> result.jsonl
     entrypoint.sh가 result.jsonl을 PostgreSQL에 적재
  -> pod 종료

[별도 실행] distill export: PostgreSQL -> Parquet -> NFS 백업 -> HF push
```

- codex exec 1회 = Q&A 1건 생성, cron 빈도로 총량 조절
- 고정 프롬프트가 codex에게 파일 읽기/데이터 생성/파일 쓰기를 지시
- 4축 분류 체계(domain x capability x difficulty x task_shape)로 데이터 다양성 관리

## 2. 디렉토리 구조

```text
distillation/
- Dockerfile                     # alpine:3.21 + codex Rust binary + Go binary
- entrypoint.sh                  # codex exec 실행 + DB 적재
- Makefile                       # lint, lint-schema, test, build, image-build, image-push, clean
- go.mod / go.sum                # Go 모듈 의존성
- cmd/
  - distill/                     # CLI entry point (urfave/cli)
    - main.go                    # app 정의, subcommand 등록
    - coverage.go                # distill coverage subcommand
    - load.go                    # distill load subcommand
    - export.go                  # distill export subcommand
- internal/
  - db/                          # 공통 DB 연결 (pgx v5, 환경변수 파싱)
  - model/                       # DistillationPair struct, references 3단계 매핑
  - coverage/                    # 4축 분포 쿼리 + coverage JSON 생성
  - loader/                      # JSONL 파싱 + CREATE TABLE + INSERT
  - exporter/                    # DB fetch + Arrow Table + Parquet shard 쓰기
- config/
  - taxonomy.yaml                # 4축 분류 체계 정의
  - settings.yaml                # 참고용 설정 문서 (코드에서 직접 읽지 않음)
- prompts/
  - distillation.md              # codex가 읽는 고정 프롬프트
- schemas/
  - distillation.schema.json     # Q&A pair JSON schema
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
- todos/                          # 이슈별 작업 문서
```

## 3. 사전 조건

- Go 1.25+
- k8s cluster
- PostgreSQL (deployments/base/postgres/ 로 함께 배포)
- NFS 서버 (백업용, `deployments/base/pvc-data.yaml`에서 서버 정보 수정)
- Codex Pro 구독 + `codex login` 인증 완료 (`~/.codex/auth.json` 존재)

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

# Codex 인증 (auth.json 기반, Pro 구독 OAuth)
# 1. 로컬에서 codex login 실행 (아직 안 했다면)
codex login --device-auth

# 2. auth.json이 생성되었는지 확인
ls -la ~/.codex/auth.json

# 3. k8s Secret 생성 (auth.json 파일 기반)
kubectl -n distillation create secret generic secret-codex \
  --from-file=auth.json=$HOME/.codex/auth.json

# 주의: auth.json의 refresh token이 만료되면 재인증 필요
# codex login --device-auth 재실행 후 Secret 업데이트:
# kubectl -n distillation delete secret secret-codex
# kubectl -n distillation create secret generic secret-codex \
#   --from-file=auth.json=$HOME/.codex/auth.json
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
# 이미지 빌드 (Go 테스트 + 로컬 cross-compile + Docker build)
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
distill export --output-dir /mnt/nfs/distillation --shard-size 50000 --compression zstd

# HuggingFace datasets push (로컬 Python 환경 또는 pipx 사용)
pipx run huggingface-cli upload YOUR_HF_REPO /mnt/nfs/distillation --repo-type dataset
```

출력 Parquet은 `train-00000-of-NNNNN.parquet` 패턴으로 HuggingFace datasets 호환. `--compression` 옵션은 zstd(기본), snappy, gzip, none 중 선택 가능.

### 7-1. export 동작 상세
- stale shard 삭제: 쓰기 전에 output-dir 내 기존 `train-*.parquet` 파일을 전부 삭제, shard 수가 줄었을 때 이전 실행의 잔여 파일이 HuggingFace datasets에 혼입되는 것을 방지
- 동시 실행 방지: output-dir에 `.write-shards.lock` 파일로 POSIX advisory lock(fcntl F_SETLK)을 획득, NFS cross-host 환경에서도 동작 (flock은 NFS에서 local-only), 이미 lock이 잡혀 있으면 즉시 실패 (EAGAIN)
- lock 파일 잔류: `.write-shards.lock` 파일은 의도적으로 삭제하지 않음, 삭제 시 TOCTOU race 발생 가능 (새 프로세스가 새 inode에 lock을 잡는 동안 기존 프로세스가 삭제된 inode의 lock을 여전히 보유하는 상황)

## 8. 로컬 개발/테스트

```bash
# Go binary 빌드
make build

# lint
make lint

# schema 검증
make lint-schema

# 테스트
make test

# Docker 이미지 빌드
make image-build

# CLI 단독 테스트 (DB 필요)
export POSTGRES_HOST=localhost POSTGRES_PORT=5432 POSTGRES_DB=distillation \
       POSTGRES_USER=distillation POSTGRES_PASSWORD=test

distill coverage --output output/coverage.json
distill load path/to/result.jsonl
distill export --output-dir ./parquet_output --compression zstd
```

