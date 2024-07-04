import os
import pathlib
from setuptools import setup
from ait.commons.util.settings import NAME, VERSION, DESC, AUTHOR, AUTHOR_EMAIL

# Directory containing this file
HERE = pathlib.Path(__file__).parent

# Text of the README file
README = (HERE / 'README.md').read_text()

# Install requirements from requirements.txt
INSTALL_REQS = [line.rstrip() for line in open(os.path.join(os.path.dirname(__file__), 'requirements.txt'))]

# Additional install requirements
ADDITIONAL_REQS = [
    'boto3>=1.23.10',
    'botocore>=1.26.10',
    'filetype==1.0.7',
    'requests>=2.20.0, <3',
    'urllib3<1.27, >=1.25.4',
    'tqdm~=4.57.0',
    'pandas~=2.2.2',
    'setuptools~=59.6.0',
    'openpyxl==3.1.3'
]

# Combine the install requirements
ALL_REQS = INSTALL_REQS + ADDITIONAL_REQS

# This call to setup() does all the work
setup(
    # dashes are ok in repo and PyPI dist names but not in package (i.e. directory) and
    # module (.py file) names. can't do import xyz-abc
    name=NAME,
    version=VERSION,
    description=DESC,
    long_description=README,
    long_description_content_type='text/markdown',
    url='https://github.com/ebi-ait/morphic-util',
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    license='Apache License',
    python_requires='>=3.10',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
    platforms=['MacOS X', 'Posix'],
    packages=['ait.commons.util', 'ait.commons.util.settings', 'ait.commons.util.command'],
    include_package_data=True,
    install_requires=ALL_REQS,
    entry_points={
        'console_scripts': [
            f'{NAME}=ait.commons.util.__main__:main',
        ]
    },
)
