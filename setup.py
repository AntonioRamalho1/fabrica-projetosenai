"""
Setup para instalação do EcoData Monitor como pacote Python
"""

from setuptools import setup, find_packages
from pathlib import Path

# Lê o README
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding='utf-8')

# Lê os requirements
requirements = (this_directory / "requirements.txt").read_text().strip().split('\n')

setup(
    name="ecodata-monitor",
    version="1.0.0",
    author="Antonio Cazé Ramalho",
    author_email="antonio.ramalho@example.com",
    description="Digital Twin + Analytics Industrial para pequenas e médias fábricas",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/AntonioRamalho1/fabrica-projetosenai",
    packages=find_packages(where="app"),
    package_dir={"": "app"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Manufacturing",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    entry_points={
        'console_scripts': [
            'ecodata-simulate=cli:simulate',
            'ecodata-etl=cli:run_etl',
            'ecodata-train=cli:train_model',
            'ecodata-dashboard=cli:run_dashboard',
        ],
    },
    include_package_data=True,
    package_data={
        'ecodata_monitor': [
            'data/**/*',
            'models/*',
            'config/*.yaml',
        ],
    },
    extras_require={
        'dev': [
            'pytest>=7.0.0',
            'pytest-cov>=3.0.0',
            'black>=22.0.0',
            'flake8>=4.0.0',
            'mypy>=0.950',
        ],
        'quality': [
            'great-expectations>=0.15.0',
            'pandera>=0.13.0',
        ],
        'viz': [
            'plotly>=5.0.0',
            'seaborn>=0.12.0',
        ],
    },
    zip_safe=False,
)