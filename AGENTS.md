
- 코드 구현 시에는 다음 스타일을 지킨다.

### Docstring

함수 · 클래스 · 모듈 정의 **바로 아래**에 `"""..."""` 문자열로 설명을 작성합니다.

```python
def add(a: int, b: int) -> int:
    """두 숫자를 더한 값을 반환한다."""
    return a + b
```

로직이 복잡하거나 인자·예외 설명이 필요하면 여러 줄로 작성합니다.

```python
def divide(a: int, b: int) -> float:
    """두 숫자를 나눈 값을 반환한다.

    args:
        a: 나눠지는 수
        b: 나누는 수 (0이면 안 됨)
    returns:
        나눗셈 결과
    raises:
        ZeroDivisionError: b가 0일 때
    """
    return a / b
```

- 여러 줄로 쓸 때는 **첫 줄에 한 줄 요약**, 한 줄 띄우고 상세 설명을 씁니다.
- 요약은 마침표로 끝나는 완결된 문장으로 작성합니다.
- 자명한 짧은 함수는 한 줄 docstring으로 충분합니다.
- 관련 표준: **PEP 257 (Docstring Conventions)**