import os

from setuptools import find_packages, setup

from djanquiltdb.djanquiltdb import __version__

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))


test_requirements = [
    'tox==4.12.1',
    'pluggy',
    'dj-database-url==2.1.0',
    'django-braces==1.15.0',
    'tblib',
    'filelock',
    'coverage',
]


dev_requirements = test_requirements + [
    'sphinx==1.8.1',
    'Jinja2>=2.3,<2.11',
    'sphinx_rtd_theme==0.4.2',
    'MarkupSafe>=0.23,<2.1.0',
]


# Since we distribute a subpackage, find_packages doesn't work for us.
setup(
    name='djanquiltdb',
    version=__version__,
    license='BSD-3-Clause',
    description="Library to shard a database on the hierarchy's top level table.",
    author='DjanQuiltDB Project; Cloud Linux Software, Inc.; Patchman B.V.',
    author_email='djanquiltdb@portal42.net; info@cloudlinux.com; hello@patchman.co',
    url='https://www.github.com/DjanQuiltDB/djanquiltdb',
    package_dir={'': 'djanquiltdb'},
    packages=find_packages('djanquiltdb', exclude=('example*', 'config*', '*test*')),
    include_package_data=True,
    install_requires=[
        'django>=6.0,<7.0',
        'psycopg[binary]>=3.0.0',
        'progressbar2',
    ],
    extras_require={
        'test': test_requirements,
        'dev': dev_requirements,
    },
    classifiers=[
        'Development Status :: 5 - Production/Stable ',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.14',
        'Framework :: Django',
        'Framework :: Django :: 6.0',
    ],
)
