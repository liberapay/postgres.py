from setuptools import setup

setup( name='postgres'
     , author='Gittip, LLC'
     , description="postgres is a high-value abstraction over psycopg2."
     , url='https://postgres-py.readthedocs.org'
     , version='1.0.0'
     , py_modules=['postgres']
     , install_requires=['psycopg2 >= 2.0.0']
      )
