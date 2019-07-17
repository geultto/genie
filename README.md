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
├── README.md
├── config : 설정 관련 폴더
├── main.py : 메인 실행
├── notebooks : 테스트용 노트북 저장 폴더
├── outputs : 결과물 저장하는 폴더
├── postprocessing.py : 후처리 스크립트
├── requirements.txt
├── slack_export.py : 슬랙 추출 스크립트
└── tests : 테스트 코드
    └── README.md : pytest 관련 자료 모음
```

### Install Environment
```
virtualenv env
source env/bin/activate
pip3 install -r requirements.txt
```

### Run
- To Be Update

### Crontab
- To Be Update



### Reference
- [slack_export Github](https://github.com/zach-snell/slack-export)