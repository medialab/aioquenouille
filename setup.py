from setuptools import setup, find_packages

with open("./README.md", "r") as f:
    long_description = f.read()

setup(
    name="aioquenouille",
    version="0.0.0",
    description="A python library of asynchronous iterator workflows.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="http://github.com/medialab/aioquenouille",
    license="MIT",
    author="Guillaume Plique, Laura Miguel",
    author_email="guillaume.plique@sciencespo.fr",
    keywords="url",
    python_requires=">=3.6",
    packages=find_packages(exclude=["test"]),
    package_data={"docs": ["README.md"]},
    zip_safe=True,
)