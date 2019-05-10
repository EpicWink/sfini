# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/07/11

"""Install ``sfini``."""

import pathlib
import setuptools

_parent = pathlib.Path(__file__).parent
_long_description = (_parent / "README.md").read_text()
_version = (_parent / "VERSION").read_text().strip()
_classifiers = [
    "Environment :: Console",
    "Intended Audience :: Developers",
    "Programming Language :: Python :: 3 :: Only",
    "Natural Language :: English",
    "Operating System :: POSIX :: Linux",
    "Operating System :: Microsoft :: Windows",
    "Operating System :: MacOS",
    "License :: OSI Approved :: MIT License"]

setuptools.setup(
    name="sfini",
    version=_version,
    license="MIT",
    author="Laurie",
    author_email="laurie@sitesee.com.au",
    maintainer="Laurie",
    maintainer_email="laurie@sitesee.com.au",
    description="Create, run and manage AWS Step Functions easily",
    long_description=_long_description,
    long_description_content_type="text/markdown",
    url="https://gitlab.com/Epic_Wink/aws-sfn-service",
    classifiers=_classifiers,
    keywords="aws sfn service step functions",
    packages=setuptools.find_packages(exclude=["tests/*"]),
    python_requires="~=3.5",
    install_requires=["boto3"],
    extras_require={
        "dev": [
            "pytest",
            "pytest-cov",
            "moto",
            "sphinx",
            "sphinx_rtd_theme"]})
