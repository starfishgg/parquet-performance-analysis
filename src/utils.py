"""
utils.py

Some reusable functions for this project.
"""

import os
import psutil
from math import isclose




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
