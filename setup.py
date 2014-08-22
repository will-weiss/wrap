from distutils.core import setup

setup(
    name='wrap',
    version='0.0.1',
    author='Will Weiss',
    author_email='will.weiss1230@gmail.com',
    packages=['wrap'],
    scripts=[],
    url='https://github.com/will-weiss/wrap/',
    license='LICENSE.txt',
    description='Enables multiprocessed reading/writing on MySQL databases',
    long_description=open('README.md').read(),
    install_requires=["numpy>=1.8.0", "MySQL-python>=1.2.4", "pandas>=0.14.0", "pandasql>=0.4.2", "impyla>=0.8.1", "psycopg2>=2.5.2", "sqlparse>=0.1.6"],
)
