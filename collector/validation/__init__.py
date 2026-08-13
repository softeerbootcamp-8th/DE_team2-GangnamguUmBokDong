"""계약 타입(types) · 정책 레지스트리(registry) · 공통 정책(policies) · 엔진(engine).

네 모듈 모두 소스 이름을 알지 못한다. 판단 기준은 전부 config에서 온다.
import 방향은 `types ← registry ← policies ← engine`으로 일직선이다.
"""

from validation import policies as _policies  # noqa: F401

# 정책 등록은 @policy / @row_policy 데코레이터의 import 부수효과다. 이 import를 지우면
# get_policy가 등록된 이름을 하나도 찾지 못하고, config의 정책 이름이 전부 맞는데도
# loader가 "미등록"으로 죽는다. 스펙 3.6절 참고.
