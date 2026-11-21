# 원천 검증과 ML 입력 인계

Python 3.12 가상환경을 만들고 저장소 루트에서 DBcheck와 AssembleDB를 함께 설치한다.

~~~sh
python -m pip install ./DBcheck ./AssembleDB pytest
python -m pip check
python -m pytest DBcheck/tests AssembleDB/tests -q
~~~

`DBcheck/configs/source.example.toml`과 `AssembleDB/configs/assembly.example.toml`에서 작업용 설정 사본을 만들고 승인된 원천 경로와 새 출력 위치를 지정한다. 두 설정을 원래 설정과 같은 디렉터리에 두면 상대 경로 기준을 유지할 수 있다. DATA 원본 및 작업용 설정은 내부에서 관리한다.

~~~sh
dbcheck check --config DBcheck/configs/source.local.toml
assemble-db build --config AssembleDB/configs/assembly.local.toml --validation-run ./DBcheck/reports/<validation-run> --purpose ml
assemble-db verify --run ./AssembleDB/outputs/<assembly-run>
assemble-db pattern-handoff --run ./AssembleDB/outputs/<assembly-run> --validation-run ./DBcheck/reports/<validation-run> --output ./handoff/<new-run>
assemble-db verify-pattern-handoff --run ./handoff/<new-run>
~~~

앞 단계가 실제로 출력한 실행 폴더명으로 자리표시자를 바꾼다. `pattern-handoff`는 출처 검증에서 확보한 사건 구성과 ML 거래 UID를 연결한다. 조립 데이터 검증은 선언한 형식·타입·값·해시를 비교한다. 기본 ML 출력은 CSV·JSON·Parquet다. 설정의 `ml_formats`로 Parquet만 선택할 수 있다. 원천과 정답을 관리 목적에 맞게 분리하고, ML 소비 코드와 서비스 DB 적재는 다른 작업으로 유지한다.
