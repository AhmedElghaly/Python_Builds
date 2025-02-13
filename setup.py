import os
from setuptools import setup, find_packages

setup(
    name="ttraft",
    version="0.2.0",
    packages=find_packages(),
    install_requires=[],
    author="Ahmed Saad",
    author_email="ahmedsaad.2feb=gmail.com",
    description="TTRaft Python Package",
    long_description=open("README.md").read() if os.path.exists("README.md") else "",
    long_description_content_type="text/markdown",
    url="https://github.com/AhmedElghaly/Python_Builds.git",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
