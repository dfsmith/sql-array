from distutils.core import setup

setup(
    name='sqlarray',
    packages=['sqlarary'],
    version='0.1',
    license='lgpl-2.1',
    description='A persistent python array backed by a SQLite3 database.',
    author='Daniel F. Smith',
    author_email='github@dfsmith.net',
    url='https://github.com/dfsmith/sql-array',
    download_url='https://github.com/dfsmith/sql-array/archive/sqlarray_v_01.tar.gz',
    keywords=['array', 'db', 'persistent'],
    install_requires=[],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: LGPL 2.1'
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
