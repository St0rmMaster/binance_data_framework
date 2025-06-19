#!/usr/bin/env python3
"""
Setup script for LeviafanDL - Universal Financial Data Framework
"""

from setuptools import setup, find_packages
import os

# Read the README file
def read_readme():
    readme_path = os.path.join(os.path.dirname(__file__), 'README.md')
    if os.path.exists(readme_path):
        with open(readme_path, 'r', encoding='utf-8') as f:
            return f.read()
    return "LeviafanDL - Universal Financial Data Framework"

# Read requirements from requirements.txt
def read_requirements():
    req_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')
    if os.path.exists(req_path):
        with open(req_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return []

setup(
    name="leviafan-dl",
    version="2.0.0",
    author="LeviafanDL Team",
    author_email="contact@leviafandl.com",
    description="Universal Financial Data Framework for multiple sources and storage backends",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/leviafandl/LeviafanDL",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Financial and Insurance Industry",
        "Intended Audience :: Developers",
        "Topic :: Office/Business :: Financial :: Investment",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
    install_requires=read_requirements(),
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
            "mypy>=1.0.0",
        ],
        "docs": [
            "sphinx>=5.0.0",
            "sphinx-rtd-theme>=1.0.0",
        ],
        "all": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0", 
            "black>=22.0.0",
            "flake8>=5.0.0",
            "mypy>=1.0.0",
            "sphinx>=5.0.0",
            "sphinx-rtd-theme>=1.0.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "leviafan-dl=leviafan_dl.cli:main",
        ],
    },
    include_package_data=True,
    package_data={
        "leviafan_dl": ["*.md", "*.txt"],
    },
    keywords=[
        "financial-data", "forex", "cryptocurrency", "stocks", "market-data",
        "dukascopy", "binance", "trading", "quantitative-finance", "data-analysis"
    ],
    project_urls={
        "Bug Reports": "https://github.com/leviafandl/LeviafanDL/issues",
        "Source": "https://github.com/leviafandl/LeviafanDL",
        "Documentation": "https://leviafandl.readthedocs.io/",
    },
)