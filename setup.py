from os.path import dirname, join

import ocelcelonis
from setuptools import setup


def read_file(filename):
    with open(join(dirname(__file__), filename)) as f:
        return f.read()


setup(
    name="ocel-celonis",
    version=ocelcelonis.__version__,
    description=ocelcelonis.__doc__.strip(),
    long_description=read_file('README.md'),
    author=ocelcelonis.__author__,
    author_email=ocelcelonis.__author_email__,
    py_modules=[ocelcelonis.__name__],
    include_package_data=True,
    packages=['ocelcelonis'],
    url='http://www.pm4py.org',
    license='Apache 2.0',
    install_requires=[
        "ocel",
        "pandas",
        "pycelonis",
        "frozendict"
    ]
)
