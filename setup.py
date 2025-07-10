from setuptools import setup, find_packages

setup(
    name="django-unmigrated-sql",
    version="0.1.0",
    description="A Django management command to execute and fake all unmigrated SQL migrations for any or all apps, ignoring errors and not using transactions.",
    author="mahiti.org",
    author_email="opensource@mahiti.org",
    url="https://github.com/mahiti/django-unmigrated-sql",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "Django>=2.2",
    ],
    classifiers=[
        "Framework :: Django",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
) 