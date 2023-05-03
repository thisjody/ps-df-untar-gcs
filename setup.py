from setuptools import setup, find_packages

setup(
    name='gcs-file-processor',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'apache-beam[gcp]',
        'google-cloud-pubsub',
        'google-cloud-bigquery',
        'google-cloud-storage',
    ],
)
