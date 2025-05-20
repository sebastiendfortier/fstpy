#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test script for fstpy package in different Python environments.
"""

import sys
# import os
# import pandas as pd
# import numpy as np


def run_test():
    """Run tests for fstpy functionality."""
    try:
        import fstpy

        print(f"\nPython version: {sys.version}")
        print(f"Testing fstpy version: {fstpy.__version__}")
        return fstpy.__version__ == "2025.03.00"

    except Exception as e:
        print(f"❌ Error during testing: {e}")
        return False


if __name__ == "__main__":
    success = run_test()
    print("✅ Great Success!" if success else "❌ Deported!")
    sys.exit(0 if success else 1)
