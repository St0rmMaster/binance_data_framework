#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name="binance_data_framework",
    version="0.1.0",
    author="AI Developer Team",
    description="Framework for downloading and storing Binance US data in Colab",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/St0rmMaster/binance_data_framework",
    packages=find_packages(),
    install_requires=[
        "python-binance",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)