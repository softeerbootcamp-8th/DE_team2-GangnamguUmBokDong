# Contributing Guide

이 저장소에 기여하는 방법을 정리한 문서입니다. PR을 올리기 전에 한 번 읽어주세요.

## Table of Contents

- [브랜치 전략](#브랜치-전략)
- [작업 절차](#작업-절차)
- [커밋 메시지 규칙](#커밋-메시지-규칙)
- [코드 스타일](#코드-스타일)
- [Pull Request 절차](#pull-request-절차)
- [체크리스트](#체크리스트)

## 브랜치 전략

- 모든 작업은 **Jira 티켓을 먼저 생성**한 후 시작합니다.
- 하나의 이슈는 **하나의 작업 브랜치**에서 처리합니다.
- `main`과 `develop` 브랜치에는 **직접 push하지 않습니다.**
- 모든 변경 사항은 **PR을 거쳐** 병합합니다.

| 브랜치 | 역할 |
| --- | --- |
| `main` | 항상 배포 가능한 상태 유지 |
| `develop` | 통합 개발 브랜치 (작업의 기준점) |
| `feature/*` 등 | 이슈별 작업 브랜치 |

> 작업 브랜치는 **항상 `develop`에서** 땁니다. `main`에서 따면 최신 개발 코드가 누락됩니다.

### 브랜치 이름 규칙

`타입/설명` 형식, **소문자 + 하이픈(kebab-case)** 을 지킵니다.

```
refactor/cart-logic
feature/login
fix/signup-error
```

타입: `feature`, `fix`, `refactor`, `docs`, `test`, `chore`

## 작업 절차

### 1. 원격 최신 정보 가져오기

새로 생긴 원격 브랜치 목록을 동기화합니다. (내 파일은 건드리지 않음)

```bash
git fetch origin
```

### 2. develop으로 이동 후 최신화

작업 기준이 되는 `develop`을 최신 상태로 맞춥니다. 이 단계를 빼먹으면 과거 코드 기준으로 작업하게 되어 충돌 위험이 커집니다.

```bash
git checkout develop
git pull origin develop
```

### 3. 작업 브랜치 생성 및 이동

최신 `develop`에서 새 브랜치를 만들고 바로 이동합니다.

```bash
# 예시: 장바구니 리팩토링
git checkout -b refactor/cart-logic
```

### 4. 개발 → 커밋 → Push

코드를 수정하고 커밋한 뒤 원격에 Push합니다. (커밋 메시지 규칙은 [아래](#커밋-메시지-규칙) 참고)

```bash
git add .
git commit -m "refactor: 장바구니 계산 로직 함수 분리 및 개선"
git push -u origin refactor/cart-logic
```

### 5. PR 생성

GitHub에서 **`develop`을 타겟으로** Pull Request를 생성합니다.

## 커밋 메시지 규칙

`타입: 설명` 형식으로 작성합니다.

```
feat: 로그인 기능 구현
fix: 회원가입 이메일 중복 검증 추가
refactor: 장바구니 계산 로직 함수 분리
```

| 타입 | 사용하는 경우 |
| --- | --- |
| `feat` | 새 기능 추가 |
| `modify` | 코드 수정 |
| `fix` | 버그 수정 |
| `refactor` | 기능 변화 없이 코드 구조 개선 |
| `docs` | 문서 수정 |
| `test` | 테스트 코드 추가 · 수정 |
| `chore` | 빌드 · 설정 · 의존성 등 기타 잡무 |

- **한 문장으로** 요약합니다.
- 타입과 설명 사이에 **`: ` (콜론 + 공백)** 을 넣습니다.
- 한 커밋에는 **하나의 논리적 변경**만 담습니다.

## 코드 스타일

팀 공통 코드 스타일 규칙입니다. 새 코드를 작성하거나 리뷰할 때 이 기준을 따릅니다.

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

### 타입 힌트 (Type Hint)

파라미터에는 `: 타입`, 함수 반환에는 `-> 반환 타입`을 붙입니다.

```python
def divide(a: int, b: int) -> float:
    return a / b
```

- 함수의 **모든 파라미터와 반환값**에 타입 힌트를 붙입니다.
- 관련 표준: **PEP 484 (Type Hints)**

### 네이밍

| 대상 | 규칙 | 예시 |
| --- | --- | --- |
| 변수 · 함수 | `snake_case` | `user_count`, `get_user` |
| 클래스 | `PascalCase` | `UserService` |
| 상수 | `UPPER_SNAKE_CASE` | `MAX_RETRY` |
| 모듈 · 파일 | `snake_case` | `user_service.py` |

- 관련 표준: **PEP 8 (Style Guide for Python Code)**

### import 정렬

아래 순서로 그룹을 나누고, 그룹 사이는 한 줄 띄웁니다.

```python
# 1. 표준 라이브러리
import os
from pathlib import Path

# 2. 서드파티 라이브러리
import httpx
from fastapi import FastAPI

# 3. 내부 패키지
from core import settings
```

## Pull Request 절차

PR을 합칠 때는 **Squash merge**를 사용합니다. 작업 중 만든 여러 커밋을 **하나로 합쳐서** `develop`에 반영하는 방식입니다.

예를 들어 작업 중 이런 커밋들이 있어도,

```
로그인 함수 구현
오타 수정
리뷰 반영
```

`develop`에는 커밋 하나로 깔끔하게 남습니다.

```
feat: 로그인 기능 구현
```

- GitHub PR 화면에서 머지 버튼의 **"Squash and merge"** 를 선택합니다.
- 히스토리가 깔끔해지고, `develop`의 커밋 하나가 곧 이슈 하나에 대응됩니다.

## 체크리스트

- 작업 브랜치는 **항상 최신 `develop`에서** 딴다.
- 브랜치 시작 전 **`git pull origin develop`** 은 필수.
- 브랜치는 **`타입/설명`** 형식(kebab-case)을 지킨다.
- 커밋 메시지는 **`타입: 설명`** 형식을 지킨다.
- 모든 함수에 **타입 힌트**와 **docstring**을 붙인다.
- 네이밍은 **PEP 8**을 따른다.
- import는 **표준 → 서드파티 → 내부** 순서로 정렬한다.
- PR 타겟은 `main`이 아니라 **`develop`**.
- PR 머지는 **Squash and merge** 로 한다.
