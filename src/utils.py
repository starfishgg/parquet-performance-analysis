"""
utils.py

Some commonly reusable functions used across the project.
"""

import os
import psutil




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

