from setuptools import setup, find_packages

packages = find_packages()
setup(
    name="spoonbill-framework",
    packages=packages,
    include_package_data=True,
    install_requires=["cloudpickle", "fsspec"],
    version="0.0.1a",
    url="https://www.xdss.io",
    description="A key-value store with different backends",
    author="Yonatan Alexander",
    author_email="jonathan@xdss.io",
    python_requires='>=3',
)
