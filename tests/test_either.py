import pytest
from itertools import product

from hypothesis import given, example
from hypothesis.strategies import integers

from fixitfelix import either


@given(integers())
def test_left_identity(i: int):
    """Checks whether the return/pure function is a left-identity for bind
    """

    either_instances = [either.Left, either.Right]
    basic_test_functions = [lambda x: 2 * x, lambda x: x + 1]
    monadic_test_functions = [
        lambda x: e(f(x))
        for e, f in product(either_instances, basic_test_functions)
    ]

    for f in monadic_test_functions:
        assert either.Left(i).bind(f) == either.Left(i)
        assert either.Right(i).bind(f) == f(i)


@given(integers())
def test_right_identity(i: int):
    """Checks whether the return/pure function is a right-identity for bind
    """
    for e in [either.Right, either.Left]:
        assert e(i).bind(lambda x: e(x)) == e(i)
    # assert either.Right(i).bind(lambda x: either.Right(x)) == either.Right(i)
    # assert either.Left(i).bind(lambda x: either.Left(i)) == either.Left(i)


@given(integers())
def test_associativity(i: int):
    """Checks whether bind is associative"""
    for e in [either.Left, either.Right]:
        f = lambda x: e(2 * x)
        g = lambda x: e(x + 1)

        assert e(i).bind(lambda x: f(x).bind(g)) == (e(i).bind(f)).bind(g)


def test_behavior_right():
    r = either.Right(1)
    res = r.bind(lambda x: either.Right(2 * x))
    assert res == either.Right(2)


def test_behavior_left():
    l = either.Left(1)
    res = l.bind(lambda x: either.Right(2 * x))
    assert res == l


def test_concatenation():
    r = either.Right(1)
    res = r | (lambda x: either.Right(2 * x)) | (lambda x: either.Right(x + 1))
    assert res == either.Right(3)

    l = either.Left(1)
    res = l | (lambda x: either.Right(2 * x)) | (lambda x: either.Right(x + 1))
    assert res == l
