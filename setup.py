from distutils.core import setup

setup(
    name='sqlarray',
    packages=['sqlarray'],
    version='0.1.0',
    license='lgpl-2.1',
    description='A persistent python array backed by a SQLite3 database.',
    author='Daniel F. Smith',
    author_email='github@dfsmith.net',
    url='https://github.com/dfsmith/sql-array',
    download_url='https://github.com/dfsmith/sql-array/archive/sqlarray_v0.1.0.tar.gz',
    keywords=['array', 'db', 'persistent'],
    install_requires=[],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Database :: Front-Ends',
        'Topic :: Software Development :: Libraries',
        'License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)',
        'Programming Language :: Python :: 3',
    ],
)
