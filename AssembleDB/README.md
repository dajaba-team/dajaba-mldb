# AssembleDB v2

DBCheck가 검증한 같은 원본을 출처별로 정제해 ML 기초 데이터와 HI-Small의 명시한 범위에 대한 서비스 적재 후보를 만듭니다. 모델 학습·추론·실제 운영 DB 쓰기는 수행하지 않습니다.

## 2026-11-01 서비스 전달 계약 변경

2026-11-02 피드백을 반영한 새 서비스 후보 출력은 `dajaba-service-candidate-v4`입니다. 서비스 키는 은행 ID의 선행 0만 제거하고 계좌번호 원문은 보존합니다. 거래별 `from_bank_id_raw`·`to_bank_id_raw`에 원본 은행 ID를 남기며, 같은 정규화 계좌의 중복 차원 행은 만들지 않습니다. 매핑에는 0 기반 `source_row_id`와 1 기반 `source_row_number`를 함께 제공합니다. 기존 v3 산출물의 정밀도 검증은 유지합니다. 기존 `transaction_id` 열을 `candidate_transaction_id`로 바꿨고, 원본 `source_file_hash`와 0부터 시작하는 `source_row_id`를 함께 보존합니다. 실제 DB의 `transaction_id`·`source_file_id`는 백엔드 발급값이며, 후보 ID·원본 행으로 추정하지 않습니다. 백엔드의 `source_row_number`와 이 출력의 행 번호 규칙을 맞춘 뒤 실제 매핑을 받아야 합니다. 기존 출력·전달 ZIP은 자동 갱신되지 않습니다.

새 v4 서비스 후보의 원본 금액은 그대로 두고 `numeric(20,6)`에 정확히 들어가는지 검사합니다. 과거 v3 산출물 감사는 당시 계약인 `numeric(18,2)`를 유지합니다. `source_manifest.json`의 `service_money_compatibility`에 검사 건수·손실 발생 건수·최대 20개 예시를 남깁니다. `BLOCKED_PRECISION_LOSS`면 팀 결정 없이 반올림하거나 적재 수락본으로 사용하지 않습니다. 값이 모두 표현 가능해도 실제 매핑·DB 수락 미검증이므로 `direct_copy_ready=false`입니다. build/verify 성공은 파일 생성·재로딩 검증이며 DB 적재 승인이 아닙니다. `verify`는 저장된 금액에서 이 검사 결과를 다시 계산합니다.

## 설치와 실행

C:/Dajaba/.venv를 공용으로 사용합니다. PyArrow와 DBCheck v2가 필요합니다.

```powershell
Set-Location C:\Dajaba
. .\DBcheck\scripts\Activate-Project.ps1
python -m pip install --no-deps --no-build-isolation -e .\AssembleDB
python -m pytest .\DBcheck\tests .\AssembleDB\tests -q
python -m dbcheck check --config .\DBcheck\configs\default.toml
```

마지막 명령이 출력한 VALIDATION_DIRECTORY 폴더를 --validation-run에 넣습니다. 아래 경로는 실제 완료한 검증 예시입니다.

```powershell
python -m assemble_db build --config .\AssembleDB\configs\default.toml --validation-run .\DBcheck\reports\20261027T004721Z_61454cc7 --purpose ml
```

--source ibm_hi_small / ibm_li_small / saml_d로 한 출처만 선택할 수 있습니다. 각 출처는 독립 릴리스이며 합쳐서 같은 거래망으로 처리하지 않습니다. 원본 정답은 labels 테이블에 분리합니다.

## 서비스 발췌

기본 configs/default.toml은 사용자 제안에 따라 HI-Small의 무작위 10,000건(mode=random_rows, count=10000, seed=42)을 중복 없이 선택합니다. 고정된 원본·같은 실행 환경·시드에서 같은 원본 행이 선택됩니다. 정답이나 모델 점수로 선별하지 않습니다. --purpose service는 서비스만, --purpose all은 ML 전체와 서비스 발췌를 함께 생성합니다.

이 무작위 발췌본은 백엔드 적재·조회·화면 연동용입니다. 연속된 거래 이력이나 탐지·평가 성능을 보장하지 않습니다. 필요한 과거 이력을 함께 제공하려면 mode=time_window로 바꾸고 start/end/history_start를 지정합니다. 별도 service_excerpt_example.toml은 이전 하루치 기술 검증 예제이며 기본 전달 범위가 아닙니다.

전체 서비스 후보는 별도 `configs/full_source.toml`의 `mode="full_source"`로 명시합니다. 전량 모드는 샘플 수·시드·시간 범위와 혼용할 수 없습니다. LI/SAML 서비스 출력은 거절합니다. 전량 파일을 생성해도 실제 DB 적재·조회 성공과 24시간 이력 완전성을 확인한 것은 아닙니다. 거래에 필요한 계좌·원본 주체, ID 대응표를 함께 제공합니다. 기간 방식은 TARGET/HISTORY를 구분하고 무작위 방식은 전부 TARGET이며 이력 완전성을 주장하지 않습니다. 실제 Source ID·DDL·COPY 수락은 별도 검증입니다.

## 결과·검증·재개

ASSEMBLY_DIRECTORY가 실행 폴더입니다. run.json의 각 source.folder 아래에 ml/service 테이블이 있고, 각 테이블에 세 형식 분할 파일·manifest.json·schema.arrow가 있습니다. 전체 행 수와 출력 범위는 source_manifest.json에서 확인합니다. 새 출력의 `service/expected.json`은 원본 행 수·선택 방식·거래/대응표/계좌/주체 건수를 백엔드 비교용으로 제공합니다. 계좌/주체는 선택 거래가 참조하는 원문 키 기준이며 전체 계좌 마스터 건수와 다를 수 있습니다. 백엔드 원본 직접 적재 건수와 후보 적재 건수를 구분해 비교해야 합니다. 룰 명세·실제 거래 ID 매핑이 확정되기 전에는 룰별 경보 기대값을 생성하지 않습니다. `verify`는 파일 지문·건수·범위 정합을 검사합니다.

```powershell
python -m assemble_db verify --run "출력된 ASSEMBLY_DIRECTORY의 실제 경로"
python -m assemble_db build --config .\AssembleDB\configs\default.toml --validation-run "사용한 검증 폴더" --purpose ml --resume "중단된 실행 폴더"
```

재개는 같은 목적·선택 출처·원본·검증·설정·코드에서만 허용합니다. 완료 출처는 재검증해 재사용하고, 미완료 출처는 새 attempt에서 다시 시작합니다. 다른 정책으로 바꿨다면 새 실행을 만드세요. 종료 코드 0은 선택 범위 출력·재로딩 성공, 2는 실패, 130은 사용자 중단입니다.

## 구조와 수정 위치

| 파일 | 역할 |
|---|---|
| src/assemble_db/config.py | 파일 상대 경로·WARN 승인·서비스 발췌 설정 |
| contracts.py | DBCheck 보고서와 원본/설정/보조 파일 지문 연결 |
| normalize.py | 출처별 컬럼·금액·계좌·정답·서비스 투영 |
| exporting.py | 세 형식 저장·NULL 표현·타입 복원·논리 검증 |
| pipeline.py | 목적별 처리·계좌/주체 연결·출처 독립 실패·재개 |
| cli.py | build/verify 실행과 종료 코드 |
| configs/default.toml | 출력 경로·배치 크기·경고별 처리·HI 발췌 |
| tests/test_assembly.py | 정밀도·범위·누수·원본 변경·변조·재개 회귀 검사 |

자세한 컬럼 의미와 직렬화 규칙은 [출력 계약](<../docs/염준영/개발문서/AssembleDB/출력계약.md>)을 확인하세요. 변경 후 pytest, ruff, pyright를 실행하고 새 입력/정책으로 작은 파일부터 검증합니다. 정밀도를 바꾸면 DBCheck 보고서도 다시 만듭니다.

## 오류 대응과 제한

- old money/identity policy 또는 config/fingerprint mismatch: 원본/설정이 맞는지 확인하고 현재 DBCheck로 재검사합니다.
- acknowledgement required: 해당 WARN의 후속 정책을 검토한 뒤 개별 규칙만 설정에 추가합니다. 모든 경고를 일괄 무시하지 않습니다.
- Service excerpt is not specified: 실제 발췌 범위를 입력하거나 --purpose ml로 독립 실행합니다.
- modified output/타입 불일치: 손상본을 완료본으로 쓰지 말고 실패 로그를 확인해 새 실행을 만듭니다.
- 공간 부족: 원본·상태 DB를 삭제하지 말고 출력 경로를 여유 있는 디스크로 바꿔 새 실행합니다.

기존 identity.sqlite3는 변경하지 않습니다. 원화 파생 환산·고객 속성 생성·행 격리는 현재 자동 처리하지 않습니다. Source 수락을 받은 적재 파일이라는 표시나 모델 성능 수치는 만들지 않습니다.

백엔드 전달 검토용 ZIP: deliveries/HI_Small_backend_random10000_seed42.zip. 원본 파일 23개의 바이트 대조와 ZIP 추출 후 verify를 통과했습니다. 외부 전송은 하지 않았습니다.

컬럼별 원본 위치·변환·결측·역할·제외 정책은 [컬럼매핑 CSV](<../docs/염준영/개발문서/AssembleDB/컬럼매핑.csv>)와 [JSON](<../docs/염준영/개발문서/AssembleDB/컬럼매핑.json>)에 108개 항목으로 정리했습니다.

## 모듈별 유지보수 가이드

- [AssembleDB 가이드 PDF](<../docs/염준영/개발문서/AssembleDB/AssembleDB_구조개편_및_유지보수가이드.pdf>)
- [편집 가능한 Markdown](<../docs/염준영/개발문서/AssembleDB/AssembleDB_구조개편_및_유지보수가이드.md>)
- [흐름도 Mermaid](<../docs/염준영/개발문서/AssembleDB/AssembleDB_구조개편_및_유지보수가이드.mmd>)

재생성: 프로젝트 루트에서 `powershell -NoProfile -ExecutionPolicy Bypass -File .\tools\Build-MaintenanceGuides.ps1`. 본문·흐름도는 Markdown에서 수정합니다. PDF 생성은 별도 문서용 Python을 사용하며, 실제 데이터 출력·학습을 다시 실행하지 않습니다.


2026-11-02 변경은 이후 새 실행에만 적용됩니다. `outputs/20261101T122831Z_e8839376`의 기존 v3 서비스 출력과 출처별 ML 출력은 수정하지 않았습니다. ML 거래·라벨 스키마와 UID·통화·금액 정밀도는 유지하므로 서비스 키 변경을 이유로 전체 ML 전처리를 반복할 필요는 없습니다. numeric(20,6)은 아직 제안이며 numeric(18,2) 정확 표현 가능성 검사와 반올림 금지는 유지합니다. 새 DBCheck/AssembleDB 실행의 시스템 시각은 UTC Z로 기록하고 원본 거래 시각은 변경하지 않습니다.

## 서비스 차원 계약 유지보수

일반 조립(`pipeline.py`)과 단계별 조립(`staged_dimensions.py`)은
`service_contract.py`의 `SERVICE_ACCOUNT_SCHEMA`·`SERVICE_ENTITY_SCHEMA`를 함께 사용합니다.
계좌·엔티티 출력 컬럼의 이름·순서·타입은 이곳에서 관리하며, 원본 계좌 연결과
서비스 계좌 정규화 방식은 그대로 유지합니다. 독립 재검증(`audit.py`)의 계산은
생산 코드와 합치지 않습니다.

이 구조 변경은 기존 산출물을 수정하거나 재생성하지 않습니다. 코드 지문은 새 실행부터
달라지므로 과거 실행과 구별합니다. 전체 원천 재실행·DB 적재·모델 학습도 수행하지 않습니다.
