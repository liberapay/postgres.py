from __future__ import absolute_import, division, print_function, unicode_literals

from setuptools import setup, find_packages

setup(
    name='postgres',
    author='Chad Whitacre et al.',
    author_email='chad@zetaweb.com',
    description="postgres is a high-value abstraction over psycopg2.",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://postgres-py.readthedocs.org',
    version='2.2.2',
    packages=find_packages(),
    install_requires=['psycopg2-binary >= 2.8', 'psycopg2-pool'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: MIT',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: SQL',
        'Topic :: Database :: Front-Ends',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
