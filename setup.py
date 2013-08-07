from setuptools import setup

setup( name='postgres'
     , description="postgres is a high-value abstraction over psycopg2."
     , version='0.0.0'
     , py_modules=['postgres']
     , install_requires=['psycopg2 >= 2.0.0']
      )
