from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))


with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='rs21-test',

    version="0.1",

    description='RS21 Backend',
    long_description=long_description,

    url='https://github.com/mgvs/rs21-test.git',

    author="Infopulse",

    license='Infopulse',

    classifiers=[
        'Development Status :: 5 - Production',
        'Environment :: Console',
        'Topic :: Software Development :: Build Tools',
        'Intended Audience :: Infopulse Developers',
        'License :: Other/Proprietary License',
        'Operation System :: POSIX :: Linux',
        'Operation System :: Unix',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
    ],

    keywords='rs21 project',

    packages=find_packages(exclude=['contrib', 'docs', 'tests']),

    install_requires=[
        'aiohttp           == 3.7.4',
        'aiohttp-swagger3  >= 0.4.4',
        'motor',
        'pyyaml',
        'vaderSentiment'
    ],

    entry_points={
        'console_scripts': [
            'rs21api = rs21_test.app.__main__:main',
        ],
    }
)
