# 웹 로그 실시간 모니터링 파이프라인 실습

producer → Kafka(`web.logs.ok` / `web.logs.error`) → consumer → monitor

## 준비물

- Docker Desktop (Windows는 WSL2 백엔드, Mac은 Intel / Apple Silicon 모두 가능)

## 한 번에 실행

```bash
docker compose up --build
```

브라우저에서 http://localhost:8000 접속 → 오른쪽 위 **주문 API 장애 발생시키기** 버튼을 눌러 보세요.

종료: `Ctrl+C` 후 `docker compose down`

## 단계별 실행 (터미널 여러 개)

```bash
docker compose build                 # 이미지 미리 빌드
docker compose up -d kafka kafka-init  # 1. Kafka + 토픽 생성
docker compose up ok-consumer        # 2. 터미널 A
docker compose up error-consumer     # 3. 터미널 B
docker compose up producer           # 4. 터미널 C
docker compose up monitor            # 5. 터미널 D → http://localhost:8000
```

## 설정 바꾸기

`.env.example`을 `.env`로 복사한 뒤 값을 수정합니다. (`.env`가 없으면 기본값으로 실행)

| 변수 | 기본값 | 의미 |
| --- | --- | --- |
| MONITOR_PORT | 8000 | 브라우저 접속 포트 |
| PRODUCER_INTERVAL_SECONDS | 0.5 | 이벤트 전송 간격(초) |
| MAX_EVENTS | 0 | 보낼 이벤트 수 (0 = 무제한) |
| ERROR_RATE | 0.03 | 평소 에러 비율 |
| FAULT_ERROR_RATE | 0.8 | 장애 모드일 때 /orders 에러 비율 |

## 처음부터 다시 하기

```bash
docker compose down
# Mac / Linux / WSL
rm -rf pipeline_output/logs pipeline_output/control
# Windows PowerShell
Remove-Item -Recurse -Force pipeline_output\logs, pipeline_output\control
```
