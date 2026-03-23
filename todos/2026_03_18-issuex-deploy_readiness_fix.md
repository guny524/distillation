# Deploy Readiness Fix
- 이슈 주소: `local-only`
- 프로젝트 전체 코드를 분석한 결과 바로 deploy 하면 동작하지 않는 구조적 문제 다수 발견
- codex 인증 flow 불완전, init/main container 볼륨 공유 깨짐, 패키지 누락 등

## 1. 배경(현재 이슈의 대략적인 이전 맥락)
- issue000에서 codex distillation pipeline 전체 설계 및 초기 구현 완료
- k8s CronJob 기반으로 codex exec를 주기적으로 실행하여 Q&A 데이터를 생성하는 구조
- 아키텍처: init container(coverage dump) -> main container(codex exec + DB 적재) -> 별도 Parquet export

## 1. 현재 이슈 및 현상, 디버그 했던 내용
### 1-1. codex CLI 인증 flow가 불완전
- cronjob.yaml:57-62에서 `CODEX_TOKEN` 환경변수를 secretKeyRef로 주입
- 하지만 entrypoint.sh에서 `codex exec` 호출 시 이 환경변수를 사용하는 코드가 없음 (--token 플래그 없음)
- codex CLI가 `CODEX_TOKEN`이라는 환경변수명을 실제로 인식하는지 프로젝트 어디에서도 검증/문서화되지 않음
- Dockerfile에서 codexuser로 실행하는데 `~/.codex/` 설정 디렉토리 초기화가 없음
- secret-codex는 kustomize secretGenerator에 포함되지 않아 `kubectl apply -k`로는 생성 안 됨

### 1-2. init container와 main container 간 coverage.json 공유 깨짐
- init container `dump-coverage`: emptyDir(`workspace-volume`)을 `/workspace/output`에 마운트
- dump_coverage.py가 `/workspace/coverage.json`에 출력 -> 이 경로는 공유 볼륨 마운트 경로(`/workspace/output`) 바깥
- init container 종료 후 `/workspace/coverage.json`은 컨테이너 파일시스템과 함께 소멸
- main container의 codex가 `/workspace/coverage.json`을 읽으려 하면 파일 부재

### 1-3. export_parquet.py 컨테이너 내 실행 불가
- Dockerfile:7-8에서 psycopg2-binary만 설치, pyarrow 미설치
- scripts/export_parquet.py:18에서 `import pyarrow` 사용
- README 7절에서 수동 실행 안내하지만 컨테이너 환경에서 실행 시 ImportError

### 1-4. 기타 불일치
- cronjob.yaml:19-20에 `output-volume: emptyDir: {}` 정의되었으나 어떤 컨테이너도 마운트하지 않음 (dead resource)
- README.md:26은 `node:22-slim` 명시, Dockerfile:1은 `node:22-bookworm` (불일치)
- postgres/deployment.yaml에 readinessProbe 미설정, CronJob init container가 PostgreSQL 준비 전 연결 시도 시 실패 가능
- Dockerfile:10에서 codex CLI 버전 미고정 (`npm install -g @openai/codex`)

### 1-5. --full-auto 헤드리스 리스크 (Codex 교차검증에서 추가 지적)
- entrypoint.sh:7에서 `--full-auto` 사용하지만, CronJob은 headless 환경
- `--full-auto`가 특정 상황에서 승인 대기할 수 있는지 확인 필요

#### 1-5-1. codex CLI 인증 방식 확인 필요
- Codex 교차검증 결과: 실제 인증은 `CODEX_API_KEY` 환경변수 또는 `~/.codex/auth.json` + `CODEX_HOME` 방식
- `CODEX_TOKEN`이라는 환경변수명은 codex CLI에서 인식하지 않음
- `codex exec --help` 출력에서 최종 확인 필요

#### 1-5-2. 교차검증 수행 정보
- Claude Opus 4.6 (1M context) + Codex 5.3 Spark (gpt-5.3-codex) 교차검증
- 양쪽 모두 일치하는 문제: C1(인증), C2(coverage.json 경로), I1(pyarrow), I3(NFS placeholder)
- Codex만 추가 지적: --full-auto 헤드리스 리스크, secret-postgres.env 0바이트 문제
- Claude만 추가 지적: I2(secret-codex kustomize 밖), R1~R4(readinessProbe, 버전 고정, dead resource, 문서 불일치)

---

## 2. 목표(해결하고자 하는 목표)
`kubectl apply -k deployments/overlays/prod/` 실행 후 CronJob이 정상 동작하는 상태로 만들기
- codex CLI 인증이 올바르게 동작해야 함
- init container가 생성한 coverage.json을 main container가 읽을 수 있어야 함
- 사용되는 Python 패키지가 전부 Dockerfile에 설치되어야 함
- 불필요한 dead resource 제거, 문서와 코드 일치

### 2-1. (사람이 생각하기에) 우선적으로 참조할 파일 (이 파일들 이외에 자율적으로 더 찾아봐야 함)
- `entrypoint.sh`
- `Dockerfile`
- `deployments/base/cronjob.yaml`
- `deployments/base/postgres/deployment.yaml`
- `prompts/distillation.md`
- `README.md`

---

(하위 부분은 사람이 작성하는게 아니라 AI 가 작성하는 부분)

# AI 결과

## 3. (AI가 생각하기에) 이슈의 원인으로 의심되는 부분들
### 3-1. codex CLI 인증: 환경변수명 불일치 (교차검증으로 높은 신뢰도)
- cronjob.yaml에서 `CODEX_TOKEN`으로 주입하지만, codex CLI가 실제로 읽는 환경변수명은 `CODEX_API_KEY` 또는 `OPENAI_API_KEY`
- 계정 인증 방식인 경우 `~/.codex/auth.json` + `CODEX_HOME` 환경변수 필요
- repo 전체에서 `CODEX_API_KEY`, `OPENAI_API_KEY`, `auth.json`, `CODEX_HOME` 관련 코드 없음 (Codex 검증 결과)

### 3-2. 볼륨 마운트 경로 설계 오류
- emptyDir이 `/workspace/output`에만 마운트되어 있어 `/workspace/coverage.json` 경로가 공유 범위 밖
- 원인: init container와 main container가 공유해야 할 파일의 경로가 볼륨 마운트 경로와 불일치하게 설계됨

### 3-3. Dockerfile 패키지 누락
- export_parquet.py가 pyarrow를 import하지만 Dockerfile에 미설치
- CronJob 자체에서 export_parquet.py를 실행하지 않으므로 CronJob 동작에는 영향 없으나, 동일 이미지로 수동 실행 시 실패

## 4. (AI가 진행한) 디버그 과정
### 4-1. 전체 파일 분석
- 프로젝트 전체 파일 (~30개)을 전부 읽어 코드 레벨에서 데이터 흐름 추적
- codex exec 호출 (entrypoint.sh) -> 인증 토큰 전달 경로 추적 -> 환경변수명 확인
- init container (cronjob.yaml) -> dump_coverage.py -> output 경로 -> volume mount 경로 비교
- main container -> prompts/distillation.md -> coverage.json 읽기 경로 확인

### 4-2. 볼륨 마운트 경로 대조
- cronjob.yaml volumes 정의: workspace-volume(emptyDir), output-volume(emptyDir), nfs-volume(PVC)
- init container volumeMounts: workspace-volume -> /workspace/output
- main container volumeMounts: workspace-volume -> /workspace/output, nfs-volume -> /mnt/nfs/distillation
- dump_coverage.py --output 기본값: /workspace/coverage.json (DEFAULT_OUTPUT 상수, dump_coverage.py:22)
- cronjob.yaml init container 명령: `--output /workspace/coverage.json` (cronjob.yaml:30-31)
- 결론: /workspace/coverage.json은 /workspace/output 볼륨 바깥 -> init-main 간 공유 안 됨

### 4-3. Dockerfile 패키지 확인
- Dockerfile:7-8: `pip install psycopg2-binary` (psycopg2만)
- scripts/export_parquet.py:18: `import pyarrow as pa` (미설치)
- scripts/export_parquet.py:19: `import pyarrow.parquet as pq` (미설치)

## 5. (AI가) 파악한 이슈의 원인
### 5-1. 치명 (deploy 해도 동작 안 함)
- C1: codex CLI 인증 환경변수명이 codex CLI의 실제 인증 메커니즘과 매칭되는지 미검증. entrypoint.sh에서 환경변수를 codex에 전달하는 코드 부재
- C2: init container의 coverage.json 출력 경로(/workspace/coverage.json)가 공유 emptyDir 마운트 경로(/workspace/output) 밖이라 main container에서 읽을 수 없음

### 5-2. 중요 (일부 기능 동작 안 함)
- I1: pyarrow 미설치로 export_parquet.py 컨테이너 내 실행 불가
- I2: secret-codex가 kustomize 관리 밖 (수동 kubectl create 필요하지만 `kubectl apply -k`와 혼용 시 실수 유발)
- I3: NFS PV 플레이스홀더 값이 TODO 상태

### 5-3. 권장 (안정성/일관성)
- R1: PostgreSQL readinessProbe 미설정
- R2: codex CLI 버전 미고정
- R3: output-volume dead resource
- R4: README에 node:22-slim이라고 적혀있지만 실제 Dockerfile은 node:22-bookworm

---

## 6. 생각한 수정 방안들
### 6-1. 방안 A: 최소 수정 (C1, C2만 해결)
- C1: `codex exec --help`로 실제 인증 환경변수명 확인 후, cronjob.yaml의 env 이름을 맞추거나 entrypoint.sh에서 환경변수 매핑
- C2: init container의 coverage.json 출력 경로를 `/workspace/output/coverage.json`으로 변경, prompts/distillation.md의 Step 2 경로도 동일하게 수정
- 범위: entrypoint.sh, cronjob.yaml, prompts/distillation.md, dump_coverage.py (DEFAULT_OUTPUT 상수)

### 6-2. 방안 B: 치명+중요 전부 해결 (C1, C2, I1, I2, I3)
- 6-1 방안 A 내용 포함
- I1: Dockerfile에 pyarrow pip install 추가
- I2: secret-codex를 kustomize secretGenerator로 관리하거나, 또는 배포 문서에 순서를 명확히 (1. secret-codex 수동 생성 -> 2. kubectl apply -k)
- I3: pvc-data.yaml의 NFS placeholder 제거하고 prod overlay에서 patch하는 구조로 변경, 또는 README에 필수 수정 사항으로 강조

### 6-3. 방안 C: 전부 해결 (C1, C2, I1, I2, I3, R1~R4)
- 6-2 방안 B 내용 포함
- R1: postgres/deployment.yaml에 readinessProbe 추가 (tcpSocket port 5432)
- R2: Dockerfile에서 codex CLI 버전 고정 (`npm install -g @openai/codex@X.Y.Z`)
- R3: cronjob.yaml에서 output-volume 정의 제거
- R4: README.md의 node:22-slim을 node:22-bookworm으로 수정, 또는 Dockerfile을 slim으로 변경

---

## 7. 최종 결정된 수정 방안 (AI 가 자동 진행하면 안되고 **무조건**/**MUST** 사람에게 선택/결정을 맡겨야 한다)
- 사용자 결정: 방안 C (전부 해결) 채택
- codex CLI 바이너리 분석으로 인증 방식 확정: `CODEX_TOKEN` 미인식, `auth.json` OAuth 방식 필요

### 7-1. C1 수정 방향: codex CLI 인증을 auth.json OAuth 방식으로 변경
- codex CLI는 `CODEX_TOKEN` 환경변수를 인식하지 않음 (바이너리 strings 분석으로 확인: `CODEX_API_KEY`, `OPENAI_API_KEY`만 존재)
- Pro 구독 quota 사용이 목적이므로 API key 방식(종량제 과금)은 부적합
- ChatGPT OAuth 방식 채택: `$CODEX_HOME/auth.json` 파일을 k8s Secret으로 관리
- auth.json에는 OAuth refresh token 포함, codex CLI가 자동으로 access token 갱신
- 주의: refresh token 자체가 만료되면 로컬에서 `codex login --device-auth` 재실행 후 Secret 업데이트 필요
- 결정 이유: 프로젝트 존재 이유가 "Pro 구독 quota 소진"이므로, API key 종량제는 목적에 반함. OAuth가 유일한 선택지

### 7-2. C2 수정 방향: coverage.json 경로를 공유 볼륨 내로 이동
- init container의 출력 경로를 `/workspace/output/coverage.json`으로 변경
- prompts/distillation.md의 Step 2, dump_coverage.py의 DEFAULT_OUTPUT, cronjob.yaml의 --output 인자를 모두 동일하게 변경
- 결정 이유: emptyDir이 `/workspace/output`에 마운트되어 있으므로, 이 경로 안에 넣어야 init-main 간 공유 가능. 볼륨 마운트 경로 자체를 바꾸면 이미지에 COPY된 파일들이 가려지는 부작용이 있으므로 파일 경로만 변경하는 것이 안전

### 7-3. I1 수정 방향: Dockerfile에 pyarrow 추가
- Dockerfile의 pip install에 pyarrow 추가하여 export_parquet.py 컨테이너 내 실행 가능하게
- 결정 이유: 동일 이미지로 CronJob(자동) + 수동 Parquet export 모두 실행 가능해야 함

### 7-4. I2 수정 방향: secret-codex를 auth.json 기반으로 변경
- 기존 `--from-literal=CODEX_TOKEN=...` 방식 폐기
- `--from-file=auth.json=~/.codex/auth.json` 방식으로 Secret 생성
- README에 생성 절차 문서화
- 결정 이유: C1과 연동, auth.json 파일 기반이므로 from-file이 적합

### 7-5. R1~R4 수정 방향
- R1: postgres/deployment.yaml에 readinessProbe 추가 (tcpSocket port 5432, initialDelaySeconds 5, periodSeconds 10)
- R2: Dockerfile에서 codex CLI 버전 고정 (`@openai/codex@0.115.0`, 현재 로컬 설치 버전)
- R3: cronjob.yaml에서 output-volume dead resource 제거
- R4: README.md의 `node:22-slim`을 `node:22-bookworm`으로 수정 (Dockerfile이 실제 구현이므로 문서를 코드에 맞춤)

### 7-6. 추가 수정: entrypoint.sh 중복 플래그 제거
- `--full-auto`가 이미 `--sandbox workspace-write`를 포함하므로 명시적 `--sandbox workspace-write` 제거
- codex exec --help 확인: `--full-auto: Convenience alias for low-friction sandboxed automatic execution (-a on-request, --sandbox workspace-write)`

---

## 8. 코드 수정 요약
- 방안 C 전부 해결: C1(인증), C2(coverage 경로), I1(pyarrow), I2(secret 변경), R1~R4(안정성)
### 8-1. C1: codex CLI 인증 수정 (auth.json OAuth 방식)
- [x] `Dockerfile:13-17`: codexuser에 `--home /home/codexuser --create-home` 추가, `ENV CODEX_HOME=/home/codexuser/.codex` + `mkdir -p` + `chown` 추가
- [x] `deployments/base/cronjob.yaml`: CODEX_TOKEN secretKeyRef 제거 -> codex-auth Secret volume + subPath mount(`/home/codexuser/.codex/auth.json`) 추가, `CODEX_HOME=/home/codexuser/.codex` env 추가
- [x] `README.md` Section 4-1: codex 인증 절차를 auth.json OAuth 기반으로 재작성 (device-auth -> auth.json 확인 -> kubectl create secret --from-file), 토큰 만료 시 재인증 절차 추가
### 8-2. C2: coverage.json 경로 수정
- [x] `scripts/dump_coverage.py:22`: DEFAULT_OUTPUT를 `/workspace/output/coverage.json`으로 변경
- [x] `deployments/base/cronjob.yaml:32`: init container의 `--output` 인자를 `/workspace/output/coverage.json`으로 변경
- [x] `prompts/distillation.md:15`: Step 2의 coverage.json 경로를 `/workspace/output/coverage.json`으로 변경
- [x] `config/settings.yaml:37`: coverage file_path를 `/workspace/output/coverage.json`으로 변경
### 8-3. I1: Dockerfile pyarrow 추가
- [x] `Dockerfile:7-8`: pip install에 `pyarrow` 추가
### 8-4. R1: PostgreSQL readinessProbe 추가
- [x] `deployments/base/postgres/deployment.yaml:29-33`: readinessProbe 추가 (tcpSocket port 5432, initialDelaySeconds 5, periodSeconds 10)
### 8-5. R2: codex CLI 버전 고정
- [x] `Dockerfile:10`: `npm install -g @openai/codex` -> `npm install -g @openai/codex@0.115.0`
### 8-6. R3: dead resource 제거
- [x] `deployments/base/cronjob.yaml`: output-volume emptyDir 정의 제거
### 8-7. R4: README 문서 일치
- [x] `README.md:26`: `node:22-slim` -> `node:22-bookworm` 수정
### 8-8. entrypoint.sh 중복 플래그 제거
- [x] `entrypoint.sh:9`: `--sandbox workspace-write \` 행 제거 (--full-auto에 이미 포함)

---

## 9. 문제 해결에 참고
- issue: `todos/2026_03_10-issuex-codex_distillation_project.md` (초기 설계 문서)
- codex CLI 인증 방식: `codex exec --help` 결과 확인 필요
- k8s 볼륨 공유: init container와 main container는 같은 Pod에서 volume을 공유하지만 컨테이너 파일시스템은 독립
- 교차검증: Codex 5.3 Spark (gpt-5.3-codex) thread_id `019cff75-bdd8-72d2-83c3-4f495a14e403`
