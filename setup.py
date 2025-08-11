import setuptools

with open("README.md", "r", encoding="utf8") as fh:
    long_description = fh.read()

requirements_list = []

LIB_NAME: str = "pharmagob-repositories-mongodb"

setuptools.setup(
    name=LIB_NAME,
    version="1.0.0",
    author="GrupoSid's Tech",
    author_email="developers@gruposid.com.mx",
    description="PharmaGob repositories, mongodb implementation",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=f"https://github.com/PharmaGobierno/{LIB_NAME}.git",
    include_package_data=True,
    keywords="pharmagob, domain, mongodb, library, python",
    packages=setuptools.find_namespace_packages(),
    package_data={"": ["*.json"]},
    namespace_packages=["pharmagob"],
    install_requires=requirements_list,
    classifiers=["Programming Language :: Python :: 3"],
    python_requires=">=3.11",
    zip_safe=True,
    test_suite="tests",
)
