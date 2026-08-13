"""fetch / normalize 어댑터 — API **제공처 단위**로 둔다.

어댑터는 소스 수만큼 필요하지 않다. 소스 7종을 어댑터 2개(seoul_openapi ·
kma_apihub)로 수용하고, 소스별 차이는 config의 `adapter_params`로 흡수한다.
"""
