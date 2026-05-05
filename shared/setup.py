from setuptools import setup, find_packages

setup(
    name="stock-shared",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "sqlalchemy>=1.4",
    ],
    python_requires=">=3.8",
)
