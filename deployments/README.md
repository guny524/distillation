# Deployments

distillation 배포 자산. k8s(kustomize)와 docker-compose(단일 호스트) 두 타겟이 같은 이미지를 재사용한다.

## 1. 사전 조건

- k8s cluster (PostgreSQL은 base에 포함 배포) 또는 docker compose 호스트
- [subgate](https://github.com/guny524/subgate) 배포: OpenAI 호환 `/v1` + `GET /quota`. baked 기본 주소가 `http://svc-subgate.subgate:8080`(`<service>.<namespace>:<port>`)라서, **subgate의 k8s Service는 반드시 이름 `svc-subgate` + namespace `subgate` + 포트 `8080`** 이어야 이 주소가 DNS로 해석된다(compose는 공유 네트워크 `subgate`의 alias `svc-subgate.subgate`). distillation 쪽 주소(`back/config/settings.yaml` roles·`back/config/opencode.json`)는 이 값으로 고정이므로 subgate Service를 여기에 맞춘다(distillation 쪽 변경 불필요)
- 이미지 빌드 도구: Go 1.25+, sqlc, jq, docker

## 2. secrets 배치

배포 전 `secrets-distillation.zip`을 **레포 루트에서** 압축 해제한다. 파일들이 kustomize secretGenerator / compose env_file이 읽는 정확한 경로에 배치된다:

```bash
unzip secrets-distillation.zip

# unzip -l secrets-distillation.zip 으로 확인 시 아래 파일들이 포함:
deployments/base/postgres/secret-postgres.env      # POSTGRES_USER/PASSWORD/DB (postgres init + 전 worker DB 접속, 전 overlay 공용)
deployments/base/secret-opencode.env               # OPENCODE_SUBGATE_API_KEY (comprehend의 opencode -> subgate)
deployments/overlays/docker-compose/.env           # compose 비밀 (POSTGRES_PASSWORD 등)
```

전부 각 디렉토리의 `.gitignore`로 git 밖이다. 값을 바꾸면 레포 루트에서 재압축해 보관한다:

```bash
zip secrets-distillation.zip deployments/base/postgres/secret-postgres.env deployments/base/secret-opencode.env deployments/overlays/docker-compose/.env
```

## 3. k8s (kustomize)

### 3-1. base 구성

- always-on worker Deployment 5종: comprehend/question/presolve/answer/verify, `distill <stage> --loop`. `POD_NAME`(downward API)이 큐 lock owner라 replica 확장이 안전하다
- 정기 CronJob 2종: ingest(`*/30`, 소스 발견), flush(매시, Parquet 반출 — `pvc-distillation-data`를 `/mnt/distillation`에 mount)
- postgres Deployment/Service/PVC, export용 plain PVC(`pvc-distillation-data`, 스토리지 백엔드는 클러스터 default StorageClass 소관)

### 3-2. 배포

```bash
make -C back image-build && make -C back image-push
# deployments/overlays/prod/kustomization.yaml의 images.newTag 갱신 후:
cd deployments/overlays/prod && make apply    # render/diff/delete 타겟도 있음
```

overlay Makefile이 `--load-restrictor LoadRestrictionsNone`을 감싼다 — distill-prompt/distill-opencode ConfigMap이 base 밖(`back/prompts/`, `back/config/`) 파일을 단일 소스로 참조하기 때문.

### 3-3. schema 변경 시 PVC wipe (drop-redeploy)

스키마는 `distill migrate`(migrate-job.yaml)가 최초 1회 `CREATE TABLE`만 한다 — 컬럼 추가/변경을 ALTER로 반영하지 않는다. 따라서 `back/internal/db/schema.sql`을 바꿨으면 기존 postgres PVC(`pvc-postgres`)를 **wipe(삭제)한 뒤 재배포**해야 새 스키마가 적용된다(이 프로젝트는 drop-redeploy 모델이라 backward-compat migration을 두지 않는다). namespace를 지웠다 다시 만들면(`make delete` 후 `make apply`) PVC도 함께 재생성돼 스키마가 새로 적용된다. 이미 반출된 학습 데이터가 필요하면 재배포 전에 export PVC(`pvc-distillation-data`)의 Parquet을 따로 보관한다.

## 4. docker-compose (단일 호스트)

```bash
cd deployments/overlays/docker-compose
docker compose up -d --build
```

- db(postgres, healthcheck) + migrate(스키마 1회 적용 후 종료, worker 게이트) + always-on worker 5종 + data-init(export 볼륨 권한) + ofelia(`daemon --docker` 라벨로 ingest `@every 30m` / flush `@every 1h` 컨테이너 스폰)
- **subgate와 같은 호스트에서 함께 띄우기**: subgate compose를 **먼저** up 한다(공유 네트워크 `subgate` 생성 + DNS alias `svc-subgate.subgate`). LLM worker 5종이 그 네트워크에 조인(external)해 baked 주소 `http://svc-subgate.subgate:8080`(base_url/quota_url)이 그대로 해석되므로 추가 설정이 없다. published 포트도 안 겹친다(distillation=5432, subgate=8080)
- subgate가 다른 호스트에 있으면 `.env`의 `SUBGATE_ENDPOINT_*`로 base_url을 덮는다. 단 quota_url은 baked라 status-check 게이트가 그 주소의 /quota에 도달해야 한다(실패 시 해당 step은 보수적으로 쉼)

## 5. config 주입 (재빌드 없이)

- 역할별 endpoint: env `SUBGATE_ENDPOINT_<ROLE>` (TEACHER/GENERATOR/STUDENT/JUDGE/TRANSLATOR/VERIFIER). 비어 있으면 이미지 baked `back/config/settings.yaml` 값 사용 (role별 기본값/제약은 settings.yaml 주석이 단일 소스)
- prompt 템플릿: `distill-prompt` ConfigMap이 `back/prompts/distillation_api.md`를 mount — 수정 후 `make apply`만으로 반영
- opencode 설정: `distill-opencode` ConfigMap이 `back/config/opencode.json`을 mount, `OPENCODE_CONFIG` env가 경로 지정
- taxonomy/schema는 Go enum·JSON schema와 결합돼 있어 이미지 baked 유지
