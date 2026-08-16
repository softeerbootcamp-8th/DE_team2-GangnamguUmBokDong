"""fetch / normalize 어댑터 — API **제공처 단위**로 둔다.

어댑터는 소스 수만큼 필요하지 않다. 소스 7종을 어댑터 2개(seoul_openapi ·
kma_apihub)로 수용하고, 소스별 차이는 config의 `adapter_params`로 흡수한다.
"""

# @adapter 데코레이터가 실행돼야 레지스트리가 채워진다. get_adapter를 호출하는
# 쪽(pipeline)이 어떤 어댑터를 import했는지 신경 쓰지 않아도 되도록, 패키지를
# import하는 시점에 둘 다 등록해 둔다. validation/__init__.py의 policies import와
# 같은 패턴이다.
from adapters import kma_apihub as _kma_apihub  # noqa: F401
from adapters import seoul_openapi as _seoul_openapi  # noqa: F401
