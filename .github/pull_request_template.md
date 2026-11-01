## 🔗 Jira
> 관련 이슈 키와 링크를 적어주세요.
- [DAJ-00](https://)

<br>

## ⭐ Key Changes
> 핵심 변경 사항을 적어주세요.
1.
2.
3.

<br>

## ✅ 확인 방법
> 리뷰어가 직접 돌려볼 수 있도록 실행 방법을 적어주세요. (`/docs` 호출 순서, curl, 테스트 이름 등)
-

<br>

## 📸 스크린샷 · 결과 (선택)
> API 응답, 테스트 결과, 생성된 STR 초안 예시 등

<br>

## 👪 To Reviewers
>

<br>

## 📎 참조한 문서 · 불변식
> 구현 전 읽은 정본 md와 지킨 규칙을 적어주세요 (사람·에이전트 공통).
> 예: `docs/str/04_facts.md` §2 · `docs/str/06_verification.md` 검사 3 · `docs/str/01_code_rules.md` 경계
-

<br>

## 📋 체크리스트
- [ ] **PR 대상이 `develop`이다** (`main`은 develop에서만 올린다)
- [ ] **로컬에서 `uv run pytest` 녹색 + `uv run ruff check .` 통과**. 실행 OS와 결과(pass/skip 수)를 위 확인 방법에 적었다
- [ ] GitHub Actions **CI가 초록불**이다
- [ ] **md가 먼저** — 동작을 바꿨다면 정본 md를 먼저 고쳤고, `docs/README.md` 상태표를 갱신했다
- [ ] (설계 결정 시) `docs/str/09_decisions.md`에 한 줄 (날짜·결정·이유·버린 대안)
- [ ] PR은 한 가지 일만 · 목표 ≤400줄 (생성 파일·녹화 응답 제외)
- [ ] 테스트 포함 — 최소 정상 1 + 경계 1 + 실패 1
- [ ] 계층 지킴 — `api → application → domain`, `domain`은 DB·LLM·시계·설정을 만지지 않는다. `str/`↔`scoring/` 직접 import 없음
- [ ] 규약 JSON 모양은 `shared/schemas/`에만
- [ ] 하드코딩 없음 — 상수는 `domain/constants.py` 또는 `.env`, 프롬프트는 템플릿, 모델 이름·매직 넘버 금지
- [ ] (프롬프트 변경 시) 기존 `*_v*.md`는 고치지 않고 새 버전 파일을 만들었다
- [ ] 로그에 계좌번호·금액·narrative 본문·프롬프트 전문이 없다
- [ ] `TODO`에 안건 번호를 달았다 (`# TODO(Open-11):`)
- [ ] (새 라이브러리 추가 시) 3줄 사유 — 유지보수 · 무게 · 라이선스, `uv.lock` 같이 커밋
