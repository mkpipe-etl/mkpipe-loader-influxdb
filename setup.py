from setuptools import setup, find_packages

setup(
    name='mkpipe-loader-influxdb',
    version='0.1.0',
    license='Apache License 2.0',
    packages=find_packages(),
    install_requires=['mkpipe', 'influxdb-client'],
    include_package_data=True,
    entry_points={
        'mkpipe.loaders': [
            'influxdb = mkpipe_loader_influxdb:InfluxDBLoader',
        ],
    },
    description='InfluxDB loader for mkpipe.',
    author='Metin Karakus',
    author_email='metin_karakus@yahoo.com',
    python_requires='>=3.9',
)
