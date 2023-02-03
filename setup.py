import os
from setuptools import setup, find_packages
from patchman_django_sharding.sharding import __version__

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))


test_requirements = [
    'tox==3.25.0',
    'dj-database-url==0.5.0',
    'django-braces==1.13.0',
    'tblib==1.3.2',
    'importlib-metadata>=0.12,<4.9.0',
    'zipp>=0.5,<3.6.0',
    'filelock<3.4.2,>=3.2',
    'importlib-resources>=1.0,<5.4.0',
    'platformdirs<2.4.1,>=2',
    'dataclasses',
    'typing-extensions<4.1.0',
]


dev_requirements = teamcity_requirements = test_requirements + [
    'sphinx==1.8.1',
    'Jinja2>=2.3,<2.11',
    'sphinx_rtd_theme==0.4.2',
    'MarkupSafe>=0.23,<2.1.0',
    'packaging<22',
    'docutils<0.19',
]


# Since we distribute a subpackage, find_packages doesn't work for us.
setup(
    name='patchman_django_sharding',
    version=__version__,
    license='BSD',
    description='Library to shard a database on the hierarchy\'s top level table.',
    author='Patchman B.V.',
    author_email='hello@patchman.co',
    url='https://github.com/sectigo/patchman-django-sharding',
    package_dir={'': 'patchman_django_sharding'},
    packages=find_packages('patchman_django_sharding', exclude=('example*', 'config*', '*test*')),
    include_package_data=True,
    install_requires=[
        'django>=1.11,<3.0',
        'psycopg2>2.7.3,<2.9.0',
        'progressbar2<4.0',
    ],
    extras_require={
        'test': test_requirements,
        'dev': dev_requirements,
        'teamcity': teamcity_requirements,
    },
    classifiers=[
        'Development Status :: 5 - Production/Stable ',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.10',
        'Framework :: Django',
    ]
)
