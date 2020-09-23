import abc
from typing import Any, Callable, TypeVar

# Note: The "type: ignore" comments make mypy ignore those lines.


class Either(abc.ABC):
    """Implementation of the Either Monad with syntax following OSlash"""

    _value: Any

    def __or__(self, func: Callable[..., "Either"]) -> "Either":
        return self.bind(func)

    @abc.abstractmethod
    def bind(self, func: Callable[..., "Either"]) -> "Either":
        ...

    @abc.abstractmethod
    def __eq__(self, other: "Either") -> bool:  # type: ignore
        ...


class Right(Either):
    """Implementation of Right instance of Either"""

    def __init__(self, value: Any):
        self._value = value

    def bind(self, func: Callable[..., Either]) -> Either:
        return func(self._value)

    def __eq__(self, other: Either) -> bool:  # type: ignore
        return isinstance(other, Right) and self._value == other._value


class Left(Either):
    """Implementation of a Left instance of Either"""

    def __init__(self, value: Any):
        self._value = value

    def bind(self, func: Callable[..., Either]) -> Either:
        return self

    def __eq__(self, other: Either) -> bool:  # type: ignore
        return isinstance(other, Left) and self._value == other._value
