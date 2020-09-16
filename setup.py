import os

from setuptools import find_packages, setup

import version

readme_path = os.path.join(os.path.dirname(__file__), "README.md")
with open(readme_path, "r") as fp:
    readme_text = fp.read()

version_for_setup_py = version.get_project_version("myhoard/version.py")
version_for_setup_py = ".dev".join(version_for_setup_py.split("-", 2)[:2])

setup(
    name="myhoard",
    version=version_for_setup_py,
    zip_safe=False,
    packages=find_packages(exclude=["test"]),
    install_requires=[
        "cryptography",
        "pghoard",
        "PyMySQL",
    ],
    extras_require={},
    dependency_links=[],
    package_data={},
    entry_points={
        "console_scripts": [
            "myhoard = myhoard.myhoard:main",
            "myhoard_mysql_env_update = myhoard.update_mysql_environment:main",
        ],
    },
    author="Rauli Ikonen",
    author_email="rauli@aiven.io",
    license="Apache 2.0",
    platforms=["POSIX"],
    description="MySQL streaming backup service",
    long_description=readme_text,
    url="https://github.com/aiven/myhoard/",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Database :: Database Engines/Servers",
        "Topic :: Software Development :: Libraries",
    ],
)
