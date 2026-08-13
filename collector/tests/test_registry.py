"""레지스트리는 이름과 함수만 다룬다. 정책의 내용은 test_policies.py가 본다."""

import pytest
from pydantic import BaseModel
from validation.registry import (
    DuplicatePolicyError,
    UnknownPolicyError,
    get_policy,
    get_row_policy,
    get_row_policy_params_model,
    is_policy_registered,
    is_row_policy_registered,
    policy,
    policy_names,
    row_policy,
    row_policy_names,
)
from validation.types import Action, RowVerdict


class SampleParams(BaseModel):
    threshold: int


def test_column_policy_round_trip(clean_registry):
    @policy("t_keep")
    def keep(value, issue, row, ctx):
        return value, Action.KEEP

    assert get_policy("t_keep") is keep


def test_decorator_returns_original_function(clean_registry):
    @policy("t_keep")
    def keep(value, issue, row, ctx):
        return value, Action.KEEP

    # 래핑하지 않으므로 테스트가 직접 호출할 수 있다
    assert keep("v", None, {}, None) == ("v", Action.KEEP)


def test_row_policy_round_trip(clean_registry):
    @row_policy("t_row")
    def always_keep(row, issues, ctx, params):
        return RowVerdict.KEEP

    assert get_row_policy("t_row") is always_keep


def test_row_decorator_returns_original_function(clean_registry):
    @row_policy("t_row_keep")
    def always_keep(row, issues, ctx, params):
        return RowVerdict.KEEP

    # 래핑하지 않으므로 테스트가 직접 호출할 수 있다
    assert always_keep({}, [], None, None) is RowVerdict.KEEP


def test_duplicate_column_policy_is_rejected(clean_registry):
    @policy("t_dup")
    def first(value, issue, row, ctx):
        return value, Action.KEEP

    with pytest.raises(DuplicatePolicyError, match="t_dup"):

        @policy("t_dup")
        def another_first(value, issue, row, ctx):
            return value, Action.KEEP


def test_duplicate_row_policy_is_rejected(clean_registry):
    @row_policy("t_dup_row")
    def first_row(row, issues, ctx, params):
        return RowVerdict.KEEP

    with pytest.raises(DuplicatePolicyError, match="t_dup_row"):

        @row_policy("t_dup_row")
        def another_first_row(row, issues, ctx, params):
            return RowVerdict.KEEP


def test_unknown_column_policy_message_lists_registered_names(clean_registry):
    @policy("t_registered")
    def known(value, issue, row, ctx):
        return value, Action.KEEP

    with pytest.raises(UnknownPolicyError) as excinfo:
        get_policy("t_typo")

    message = str(excinfo.value)
    assert "t_typo" in message
    assert "t_registered" in message  # 고치는 주체는 사람이다 — 목록을 보여준다


def test_unknown_row_policy_raises(clean_registry):
    with pytest.raises(UnknownPolicyError, match="t_missing"):
        get_row_policy("t_missing")


def test_unknown_row_policy_message_lists_registered_names(clean_registry):
    @row_policy("t_registered_row")
    def known(row, issues, ctx, params):
        return RowVerdict.KEEP

    with pytest.raises(UnknownPolicyError) as excinfo:
        get_row_policy("t_typo_row")

    message = str(excinfo.value)
    assert "t_typo_row" in message
    assert "t_registered_row" in message  # 고치는 주체는 사람이다 — 목록을 보여준다


def test_registries_are_not_crossed(clean_registry):
    @policy("t_column_only")
    def column(value, issue, row, ctx):
        return value, Action.KEEP

    @row_policy("t_row_only")
    def row_fn(row, issues, ctx, params):
        return RowVerdict.KEEP

    # 계약(인자 개수·반환 타입)이 달라 섞어 조회하면 실행 중에 터진다
    with pytest.raises(UnknownPolicyError):
        get_row_policy("t_column_only")
    with pytest.raises(UnknownPolicyError):
        get_policy("t_row_only")


def test_is_registered_does_not_call_the_function(clean_registry):
    @policy("t_boom")
    def boom(value, issue, row, ctx):
        raise AssertionError("정책이 실행되면 안 된다")

    assert is_policy_registered("t_boom") is True
    assert is_policy_registered("t_absent") is False
    assert is_row_policy_registered("t_boom") is False


def test_params_model_lookup_returns_model(clean_registry):
    @row_policy("t_with_params", params=SampleParams)
    def with_params(row, issues, ctx, params):
        return RowVerdict.KEEP

    assert get_row_policy_params_model("t_with_params") is SampleParams


def test_params_model_lookup_returns_none_when_policy_takes_no_params(clean_registry):
    @row_policy("t_no_params")
    def no_params(row, issues, ctx, params):
        return RowVerdict.KEEP

    # None은 "등록됐고 params를 받지 않는다"는 뜻이다. #2가 이 값으로
    # "params를 받지 않는 정책에 row_params가 왔다"를 판정한다.
    assert get_row_policy_params_model("t_no_params") is None


def test_params_model_lookup_raises_for_unknown_policy(clean_registry):
    with pytest.raises(UnknownPolicyError):
        get_row_policy_params_model("t_absent")


def test_name_listings_are_sorted(clean_registry):
    @policy("t_b")
    def b(value, issue, row, ctx):
        return value, Action.KEEP

    @policy("t_a")
    def a(value, issue, row, ctx):
        return value, Action.KEEP

    names = policy_names()
    assert names.index("t_a") < names.index("t_b")
    assert isinstance(row_policy_names(), tuple)
