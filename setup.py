from setuptools import setup, find_packages

setup( name='postgres'
     , author='Gittip, LLC'
     , description="postgres is a high-value abstraction over psycopg2."
     , url='https://postgres-py.readthedocs.org'
     , version='2.0.0'
     , packages=find_packages()
     , install_requires=['psycopg2 >= 2.5.0']
      )
