from setuptools import setup, find_packages

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='gtfs-rt-to-tides',
    version='0.0.1',
    author='Evan Siroky',
    description='A few tools to download GTFS-RT data and summarize/transform it into TIDES data',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/evansiroky/gtfs-rt-to-tides',
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
    install_requires=required,
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'download=downloader:main',
            'parse_trips=parse_trips:main'
        ]
    },
)
