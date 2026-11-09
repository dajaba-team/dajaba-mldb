# DBCheck v2 — 원본 품질 검증

DBCheck는 IBM HI-Small·IBM LI-Small·SAML-D 원본을 **각각 독립적으로 검사**합니다. 원본 수정, 정규화 데이터 생성, 행 삭제, 모델 학습은 수행하지 않습니다. 기존 벤치마크·Accuracy 승인·재학습 명령은 폐기했습니다.

## 구조

```text
DBcheck/
  configs/default.toml       # 출처·경로·계약·작업자 스레드 수
  configs/currencies.json    # 확인된 통화 별칭과 미확인 값
  src/dbcheck/
    cli.py                  # check 명령과 종료 코드
    config.py               # 설정 검증·출처 계약·지문
    data_contract.py        # 원문 키·계좌 별칭·정확한 금액·통화 연산 제약
    distribution.py         # 원본 배치 읽기·일별/시간별 집계
    checks.py               # 명시적 품질·중복·계좌 참조 검사
    pipeline.py             # 출처별 독립 실행·오류 격리
    reporting.py            # 보고서 계약·판정·관측 건수·요약
    __init__.py / __main__.py
  tests/test_validation.py  # 오류·정상·출처 격리 회귀 검사
  reports/<run_id>/<source>/
```

의존성은 PyArrow와 Polars입니다. PyArrow는 원본을 배치로 읽고, Polars는 원본 값의 집계·중복 후보·계좌 참조를 검사합니다. 상세 검사는 출처별 작업자 프로세스에서 실행하여 네이티브 종료도 다른 출처를 중단하지 않게 했습니다. 하드 메모리 상한을 보장하는 설정은 제공하지 않습니다. 작은 배치·스트리밍 집계·단일 작업자 스레드가 기본입니다.

## 설치와 실행

프로젝트 루트 `C:\Dajaba`에서 실행합니다.

```powershell
.\.venv\Scripts\python.exe -m pip install -e .\DBcheck
.\.venv\Scripts\python.exe -m dbcheck check --config .\DBcheck\configs\default.toml
```

한 출처만 검사하거나 여러 `--source`를 지정할 수 있습니다.

```powershell
.\.venv\Scripts\python.exe -m dbcheck check --config .\DBcheck\configs\default.toml --source ibm_hi_small
.\.venv\Scripts\python.exe .\tools\manual_pipeline.py dbcheck --source saml_d
```

설정의 파일 경로는 TOML 파일 위치를 기준으로 해석합니다. 보고서 위치가 원본 디렉터리 안이면 실행을 거부합니다. 인터넷 다운로드·환율 조회·모델 적합은 실행 경로에 없습니다. `.venv`에 과거 ML 패키지가 남아 있어도 DBCheck v2는 사용하지 않습니다.

## 검사와 판정

- 스키마·중복된 IBM Account 헤더 위치·필수값·정확한 달력 날짜·라벨
- 빈 문자열·UNKNOWN·해당 없음 토큰을 별도 집계
- 원문 금액 형식·0 금액·출처별 Decimal 표현 가능성(IBM 38,12 / SAML 38,16)
- 통화 별칭·결제 수단·SAML 단일 금액의 통화 불확실성
- 은행 ID 형식·IBM 원본 계좌 참조·0 패딩 후보키와 소유 주체 충돌
- 원본 모든 컬럼이 같은 중복 후보. 근거 없이 삭제하지 않음
- 전체 일별·시간별 정상/양성·관측 공백·파일 순서 역전·정상 거래량 감소 후보
- 거래·계좌 원본의 읽기 전후 SHA256, 설정·별칭·코드 지문

PASS는 해당 규칙에서 문제가 검출되지 않았다는 뜻입니다. WARN은 검토·정책 확인이 필요하며 데이터 삭제를 뜻하지 않습니다. FAIL은 해당 출처의 계약 위반 또는 검사 미완료입니다. 다른 출처는 계속 검사합니다. 영향 건수가 행·셀·시간·계좌키 그룹 중 무엇인지는 `count_unit`을 함께 읽으세요. 외부 기대 기간이나 SAML 고객 정보처럼 확인할 수 없는 항목은 영향 건수 NULL로 표시합니다.

종료 코드는 전체 출처에 FAIL이 없으면 0, 설정 오류/출처 FAIL은 2, 사용자 중단은 130입니다. WARN을 서비스 적재 승인으로 해석하면 안 됩니다.

## 산출물과 AssembleDB 인계 계약

`reports/<run_id>/run.json`은 실행·설정·별칭·코드·패키지 버전과 출처별 완료 상태를 기록합니다. 각 출처 폴더에는 다음 파일이 있습니다.

| 파일 | 역할 |
|---|---|
| validation_report.json | `dajaba-source-validation-v2` 계약, PASS/WARN/FAIL, 원본 지문·건수, 규칙·영향·예시·조치 |
| `<source>_profile.json` | 원본 헤더·라벨 의미·기간·분포·결측·한계·원본 해시 대조 |
| `<source>_daily.csv` / `<source>_hourly.csv` | 전체 기간의 날짜·시간별 건수. 공백 시간도 0건으로 표시 |
| detail.log | 상세 검사 작업자의 단계·실패 원인 |
| detail_result.json 또는 detail_error.json | 작업자 결과 또는 오류 |
| summary.md | 사람이 읽는 규칙별 결과 |

AssembleDB v2는 Phase 3에서 구현을 완료했고 이 보고서를 소비합니다. `full_scan_complete=true`, 계약 버전, 현재 입력·보조 파일의 SHA256을 대조해야 합니다. FAIL은 차단하고 WARN은 명시적 정책 확인 대상으로 처리합니다. 과거 stage 캐시나 Accuracy 비교 보고서는 사용하지 않습니다.

DBCheck 결과에는 원본 정규화 Parquet이 없습니다. AssembleDB는 검증 지문과 동일한 원본을 직접 읽어 변환해야 합니다. CSV·JSON·Parquet 출력·재로딩 검증은 AssembleDB에서 완료했습니다. 실제 PostgreSQL COPY 수락은 Source와 별도로 확인해야 하며 아직 수행하지 않았습니다.

## 검증·수정·재실행

```powershell
.\.venv\Scripts\python.exe -m pytest .\DBcheck\tests -q
.\.venv\Scripts\python.exe -m ruff check .\DBcheck\src .\DBcheck\tests .\tools\manual_pipeline.py
```

전체 원본 검증 근거는 `DBcheck/reports/20261027T004721Z_61454cc7`, 로그는 `artifacts/redesign/phase3/dbcheck_current.log`에서 확인합니다. 최종 리뷰 보완과 회귀 결과는 `artifacts/redesign/phase3/execution_report.json`을 함께 읽으세요. 테스트 fixture 통과와 전체 원본 검증 완료는 별도 상태입니다.

중단된 실행을 완료 보고서로 재사용하지 않습니다. 같은 명령을 다시 실행하면 새 run_id로 검사합니다. 캐시는 재사용하지 않으므로 원본·설정 변경 후 오래된 결과가 자동 채택되지 않습니다.

| 바꾸려는 내용 | 수정할 파일 | 확인할 사항 |
|---|---|---|
| 데이터셋·경로·기대 기간 | configs/default.toml | 출처별 실제 CLI 재실행 |
| 라벨 의미·원본 계약 | config.py, distribution.py | 헤더·파싱 오류 회귀 검사 |
| 통화 별칭 | configs/currencies.json | 원본 보존·미확인 별칭 WARN |
| 금액·식별자·참조·중복 규칙 | checks.py | 정상/오류 fixture와 전체 해당 출처 |
| 시간 집계·후반부 후보 | distribution.py | 시간 공백·달력 날짜·건수 대조 |
| 보고서 계약 | reporting.py | 계약 버전 갱신 및 Phase 3 소비자 변경 |

스키마가 다르면 헤더를 덮어 고치지 말고 파일 버전과 설정을 확인합니다. 상세 작업자 실패 시 `detail.log`와 종료 코드를 먼저 확인합니다. 원본 계좌 참조가 깨지면 거래와 계좌 파일이 같은 변형·버전인지 확인합니다. 기간 후보·미확인 통화는 원본 설명을 대조하고, 제외·환산은 DBCheck에서 실행하지 않습니다.

기존 코드·테스트·설정의 복구본은 `artifacts/redesign/phase1/20261026T155911Z/prechange_code_config.zip`에 있습니다. 필요하면 별도 폴더에 풀고 `snapshot_manifest.json`과 해시를 대조해 선택 복구합니다. Phase 4 모듈별 가이드는 아래 PDF와 편집 원본으로 제공합니다.

## 2026-10-27 적재 전 계약 보완

SAML 전용 금액 정밀도를 Decimal(38,16)으로 지정했습니다. 서비스 대상 IBM은 Decimal(38,12)를 유지합니다. `amount_precision`·`amount_scale`은 출처 설정에서 검증하며, 초과 값을 반올림하지 않습니다.

`data_contract.py`는 외부 은행/계좌 키의 0을 보존하고, 검증된 출처 내부 연결에만 별도 숫자형 은행 별칭 키를 사용합니다. 보고서의 `bank_identity_bridge.mapping_unique_and_complete`에서 전체 참조와 유일성 검증을 확인합니다.

미확인 통화는 출처별 native bucket으로 유지하고 ISO를 추측하지 않습니다. FX는 차단합니다. SAML 단일 Amount의 두 통화가 다르면 금액 단위를 미확정으로 유지하여 합산·환산을 차단하고 시간·건수·관계 데이터는 보존합니다. 경고를 삭제해 해결한 것으로 표시하지 않습니다.

보고서에 `exact_amount_contract`, `bank_identity_bridge`, `currency_contract`, `money_identity_policy_version`을 추가했습니다. Source 문서가 아직 미정으로 둔 서비스 거래 ID 시작 번호와 PostgreSQL 최종 타입은 팀 확정이 필요합니다. 실제 COPY 성공은 확인하지 않았습니다. 상세 조치: 문서관리/다자바/공통 설계/AI/데이터파이프라인_개편/Phase2_미해결항목_조치.md.

## 모듈별 유지보수 가이드

- [DBCheck 가이드 PDF](<../docs/염준영/개발문서/DBcheck/DBCheck_구조개편_및_유지보수가이드.pdf>)
- [편집 가능한 Markdown](<../docs/염준영/개발문서/DBcheck/DBCheck_구조개편_및_유지보수가이드.md>)
- [흐름도 Mermaid](<../docs/염준영/개발문서/DBcheck/DBCheck_구조개편_및_유지보수가이드.mmd>)

재생성: 프로젝트 루트에서 `powershell -NoProfile -ExecutionPolicy Bypass -File .\tools\Build-MaintenanceGuides.ps1`. Markdown 본문과 그 안의 Mermaid 블록이 편집 원본이며 .mmd는 같은 흐름도를 별도 저장한 사본입니다. 생성 후 두 PDF를 각각 렌더링해 확인합니다.

최종 상태: DBCheck 41개, AssembleDB 포함 52개 회귀 통과. 기존 21,507,246건의 전수 검사 이후 EMPTY_SOURCE 가드를 정리했고 현재 회귀 검사에서 빈 파일·미확정 계좌·통화 차단을 확인했습니다. 실제 전체 출력과 모델 학습은 구분합니다.

## 원본 관측 건수 인계 (2026-11-01)

새 검사에는 출처별 `source_expected.json`이 추가됩니다. 전체 원본 거래 건수·원본 SHA-256·보조 원본 관측 건수·시각 범위를 기록하고 `validation_report.json`에 지문을 연결합니다. FAIL 상태의 관측값도 진단 근거이며 적재 허가가 아닙니다. 정규화 데이터나 룰별 경보 정답을 생성하지 않고, 실제 PostgreSQL 적재 수락 여부는 미검증으로 표시합니다. AssembleDB는 이 파일이 있는 새 보고서의 지문·원본 연결을 검사합니다. 기존 완료 보고서는 계속 사용할 수 있지만 변경된 원본/설정은 재검증해야 합니다. 검증 당시 있던 보조 원본이 사라지거나 새 파일이 추가되면 AssembleDB가 연결을 거부합니다.


## 2026-11-02 인터페이스 피드백 반영

`service_account_key()`는 은행 ID의 선행 0만 제거합니다. 예: `010` + `000ABC` → `10_000ABC`. 원본 파일·계좌번호·ML 내부 연결 키는 변경하지 않습니다. 원본 은행 표기는 AssembleDB v4 서비스 거래의 별도 raw 필드에 남깁니다. 기존 원본 검증 정책 버전(v2)과 보고서는 보존하고, 서비스 출력 계약만 별도로 v4로 구분합니다. 새 실행의 시스템 시각은 UTC Z로 기록합니다.
