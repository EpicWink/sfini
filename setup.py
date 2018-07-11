# --- 80 characters -------------------------------------------------------
# Created by: Laurie 2018/08/11

"""Install ``sfeeny``."""

import pathlib
import setuptools

_parent = pathlib.Path(__file__).parent
_long_description = _parent.joinpath("README.md").read_text()
_version = _parent.joinpath("VERSION").read_text().strip()
_classifiers = [
    "Environment :: Console",
    "Intended Audience :: Developers",
    "Programming Language :: Python :: 3 :: Only",
    "Natural Language :: English",
    "Operating System :: POSIX :: Linux",
    "Operating System :: Microsoft :: Windows"]

setuptools.setup(
    name="sfeeny",
    version=_version,
    author="Laurie",
    author_email="laurie@sitesee.com.au",
    maintainer="Laurie",
    maintainer_email="laurie@sitesee.com.au",
    description="AWS SFN Service",
    long_description=_long_description,
    url="https://gitlab.com/Epic_Wink/aws-sfn-service",
    classifiers=_classifiers,
    keywords="aws sfn service step functions",
    packages=setuptools.find_packages(exclude=["tests/*"]),
    install_requires=["boto3"],
    extras_require={
        "dev": ["pytest", "pytest-cov", "mock", "moto"]})
