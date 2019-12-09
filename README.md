# genie
- 글또의 요정 지니 프로젝트
- 글또 과제 제출 / 상호 피드백 확인을 도와줌


### 기본 아이디어
- 주기적으로 슬랙에서 데이터를 가지고 옴
- 가지고 온 데이터에서 특정 조건(:submit:, :feedback: 이모지)을 만족하는 URL 추출
- 데이터 처리한 후, BigQuery에 저장
- BigQuery에서 스프레드시트로 동기화

### Architecture
```
├── config : 설정 관련 데이터
├── outputs : slack에서 가져온 데이터 저장
├── notebooks : 테스트용 노트북 저장 폴더
├── main.py : 메인 실행 스크립트
├── slack_export.py : slack 데이터 추출 스크립트
├── checker.py : slack message에서 글 제출 확인 로직
├── extract_data.py : bigquery 및 json data 처리
└── tests : 테스트 코드
    └── README.md : pytest 관련 자료 모음
```

### Install Environment
```
virtualenv env
source env/bin/activate
pip3 install -r requirements.txt
```

### Slack Token 환경 변수 지정
- Terminal에서

    ```
    export SLACK_TOKEN='your_token'    
    ```

### Run
#### \# `main.py` 실행

```
python main.py --channel_prefix 3_
               --gbq_phase production
               --deadline 2019-07-22
               --run_all False
```
  - `channel_prefix` : 추출하기 원하는 채널의 접두사 (ex. '1_', '2_' 등)
  - `gbq_phase` : 실행시키는 용도 ('development' 또는 'production'으로 입력)
  - `deadline` : 현재 제출의 마감 기한 (year-month-day 형태로 입력) (추후 crontab으로 자동화하면서 직접 입력해 줄 필요 없어질 예정)
  - `run_all` : True - 모든 deadline에 대해 체크 / False - 이번 deadline에 대해서만 체크


### Crontab
- To Be Update



### Reference
- [slack_export Github](https://github.com/zach-snell/slack-export)
