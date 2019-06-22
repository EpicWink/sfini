# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/07/11

"""Install ``sfini``."""

import pathlib
import setuptools

parent = pathlib.Path(__file__).parent
long_description = (parent / "README.md").read_text()
version = (parent / "VERSION").read_text().strip()
classifiers = [
    "Environment :: Console",
    "Intended Audience :: Developers",
    "Programming Language :: Python :: 3 :: Only",
    "Natural Language :: English",
    "Operating System :: POSIX :: Linux",
    "Operating System :: Microsoft :: Windows",
    "Operating System :: MacOS",
    "License :: OSI Approved :: MIT License"]
extras_require = {
    "dev": [
        "pytest",
        "pytest-cov",
        "pytest-timeout",
        "moto",
        "sphinx",
        "sphinx_rtd_theme"]}
project_urls = {
    "Documentation": "https://sfini.readthedocs.io/en/latest/",
    "Source": "https://github.com/EpicWink/sfini",
    "Bugs": "https://github.com/EpicWink/sfini/issues"}
packages = setuptools.find_packages(where="src")

setuptools.setup(
    name="sfini",
    version=version,
    license="MIT",
    author="Laurie",
    author_email="laurie@sitesee.com.au",
    maintainer="Laurie",
    maintainer_email="laurie@sitesee.com.au",
    description="Create, run and manage AWS Step Functions easily",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/EpicWink/sfini",
    classifiers=classifiers,
    keywords="aws sfn service step functions states",
    packages=packages,
    package_dir={"": "src"},
    python_requires="~=3.6",
    install_requires=["boto3"],
    extras_require=extras_require,
    project_urls=project_urls)
