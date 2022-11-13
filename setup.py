from setuptools import setup, find_packages

setup(name='Isql_v2',
      version='0.1',
      url='https://github.com/ajcltm/Isql_v2',
      license='jnu',
      author='ajcltm',
      author_email='ajcltm@gmail.com',
      description='',
      packages=find_packages(exclude=['test']),
      zip_safe=False,
      setup_requires=['requests>=1.0'],
      test_suite='test.test_sql')