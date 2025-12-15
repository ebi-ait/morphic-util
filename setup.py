import pathlib
from setuptools import setup, find_packages
from ait.commons.util.settings import NAME, VERSION, DESC, AUTHOR, AUTHOR_EMAIL

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()
INSTALL_REQS = (HERE / "requirements.txt").read_text().splitlines()

setup(
    name=NAME,
    version=VERSION,
    description=DESC,
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/ebi-ait/morphic-util",
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    license="Apache License",
    python_requires=">=3.10",
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    packages=find_packages(include=["ait*"]),
    include_package_data=True,
    install_requires=[r for r in INSTALL_REQS if r.strip() and not r.strip().startswith("#")],
    entry_points={"console_scripts": [f"{NAME}=ait.commons.util.__main__:main"]},
)

