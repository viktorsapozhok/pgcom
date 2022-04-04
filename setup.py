from os import path
from setuptools import setup

version = '0.2.9'

root_dir = path.abspath(path.dirname(__file__))

try:
    with open(
            path.join(root_dir, 'README.md'),
            mode='r',
            encoding='utf-8'
    ) as f:
        long_description = f.read()
except IOError:
    long_description = ''

version_path = path.join(root_dir, 'pgcom', 'version.py')
with open(version_path, mode='w') as f:
    f.write('__version__ = "{}"\n'.format(version))


def get_requirements():
    r = []
    with open("requirements.txt") as fp:
        for line in fp.read().split("\n"):
            if not line.startswith("#"):
                r += [line.strip()]
    return r


setup(
    name='pgcom',
    version=version,
    description='PostgreSQL communication manager',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Alex Piskun',
    author_email='piskun.aleksey@gmail.com',
    url='https://github.com/viktorsapozhok/pgcom',
    packages=['pgcom'],
    include_package_data=True,
    zip_safe=False,
    install_requires=get_requirements(),
    extras_require={
        'test': ['pytest', 'tox', 'black'],
        'docs': ['sphinx', 'sphinx_rtd_theme', 'sphinx-autodoc-typehints']
    },
    python_requires='>=3.7',
)
