"""Install ``sfini``."""

import pathlib
import setuptools

parent = pathlib.Path(__file__).parent
long_description = (parent / "README.md").read_text()
version = (parent / "VERSION").read_text().strip()

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
    classifiers=[
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3 :: Only",
        "Natural Language :: English",
        "Operating System :: POSIX :: Linux",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS",
        "License :: OSI Approved :: MIT License"],
    keywords="aws sfn service step functions states",
    packages=setuptools.find_packages(where="src"),
    package_dir={"": "src"},
    python_requires="~=3.6",
    install_requires=["boto3"],
    extras_require={
        "dev": [
            "pytest",
            "pytest-cov",
            "pytest-timeout",
            "moto",
            "sphinx",
            "sphinx_rtd_theme"]},
    project_urls={
        "Documentation": "https://sfini.readthedocs.io/en/latest/",
        "Source": "https://github.com/EpicWink/sfini",
        "Bugs": "https://github.com/EpicWink/sfini/issues"})
