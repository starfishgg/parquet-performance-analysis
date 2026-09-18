"""
utils.py

Some reusable functions for this project.
"""

import os
import psutil

from math import isclose

from statistics import median
from time import perf_counter
from typing import Callable


# Constant for repeated runs and calculating a median from them.
REPETITIONS = 3


def print_section(title: str) -> None:
    """
    Print a consistent section heading for reporting.
    """

    print()
    print("=" * 60)
    print(title)


def print_memory_usage() -> None:
    """
    Print the current Python process memory usage.
    """
    
    print(f"Memory usage: {get_memory_usage_mb():.2f} MB")


def get_memory_usage_mb() -> float:
    """
    Return the current Python process memory usage in MB.
    """

    process = psutil.Process(os.getpid())

    return process.memory_info().rss / (1024 * 1024)


def values_are_equal(first: float, second: float) -> bool:
    """
    Return True if two floating-point values are equal within
    the tolerance used for floating-point rounding differences.
    """
    return isclose(
        first,
        second,
        rel_tol=1e-9,
        abs_tol=0.01,
    )


def benchmark_function(
    function: Callable[[], object],
    repetitions: int = 3,
) -> tuple[object, float]:
    """
    Warm up a function and return the median execution time
    from the measured runs.
    
    The warp-up run is not included in the result, as we have seen
    from previous tests, the first run can give a much slower values then the followups (several seconds)
    """

    if repetitions < 1:
        raise ValueError("repititions must be at least 1")

    # Warm-up fun.
    function()

    elapsed_times = []
    final_result = None

    for _ in range(repetitions):
        start_time = perf_counter()

        final_result = function()

        elapsed_time = perf_counter() - start_time
        elapsed_times.append(elapsed_time)

    return final_result, median(elapsed_times)
