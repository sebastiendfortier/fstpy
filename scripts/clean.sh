#!/bin/bash

echo "Starting cleanup process..."

# Remove Python build artifacts
echo "Removing Python build artifacts..."
echo "  - Removing build/, dist/, *.egg-info/, __pycache__/, .eggs/, .pytest_cache/, .coverage, .pixi, and package_tests/environments/.pixi "
rm -rf build/ dist/ *.egg-info/ __pycache__/ .eggs/ .pytest_cache/ .coverage .pixi package_tests/environments/.pixi

echo "  - Cleaning up all __pycache__ directories..."
find . -type d -name __pycache__ -exec rm -rf {} \; 2>/dev/null || echo "    No __pycache__ directories found"

echo "  - Cleaning up all .egg-info directories..."
find . -type d -name "*.egg-info" -exec rm -rf {} \; 2>/dev/null || echo "    No .egg-info directories found"

# Remove documentation build
echo "Removing documentation build artifacts..."
echo "  - Removing doc/_build/"
rm -rf doc/_build/
echo "  - Running make clean in doc directory if Makefile exists..."
(cd doc && if [ -f Makefile ]; then make clean; else echo "    No Makefile found in doc directory"; fi)

# Remove conda build artifacts
echo "Removing conda build artifacts..."
echo "  - Removing /tmp/conda-build/"
rm -rf /tmp/conda-build/

echo "Cleanup complete! All temporary and build files have been removed." 
