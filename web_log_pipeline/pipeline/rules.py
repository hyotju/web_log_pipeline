"""이벤트를 ok / error 로 판정하는 규칙. 과제에서 이 파일을 고쳐 본다."""


def classify(event: dict) -> str:
    # 5xx(서버 에러)면 error, 나머지(2xx, 3xx, 4xx)는 ok
    if event["status"] >= 500:
        return "error"
    return "ok"
