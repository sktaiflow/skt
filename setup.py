import setuptools

setuptools.setup(
    name="skt",
    version="0.1.7",
    author="SKT",
    author_email="all@sktai.io",
    description="SKT package",
    url="https://github.com/sktaiflow/skt",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        'hvac>=0.9.6',
        'pyhive[hive]',
        'pyarrow',
        'pandas',
    ],
)
