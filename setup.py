import sys
from setuptools import setup


if sys.version_info < (3, 4):
    sys.stderr.write('This module requires at least Python 3.4\n')
    sys.exit(1)

require_libs = ['elasticsearch>=6.3.1', 'kafka-python>=1.4.4', 'myconf>=1.1.0']


with open('README.md', encoding='utf-8') as f:
    long_description = f.read().strip()


setup(
    name='kfk2es',
    description='get data from kafka, after processing send them to elasticsearch',
    long_description = long_description,
    long_description_content_type = 'text/markdown',
    version='1.1.0',
    author='thuhak',
    author_email='thuhak.zhou@nio.com',
    keywords='kafka elasticsearch logstash',
    packages =['kfk2es'],
    url='https://github.com/thuhak/kfk2es',
    install_requires=require_libs
)
