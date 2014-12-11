from setuptools import find_packages, setup

from bynamodb.version import VERSION


install_requires = ['boto']

setup(
    name='bynamodb',
    version=VERSION,
    url='https://github.com/teddychoi/BynamoDB',
    author='Bochul Choi',
    author_email='vio.bo94@gmail.com',
    license='MIT',
    description='High level DynamoDB API'
                'wrapping boto.dynamodb2.layer1.DynamoDBConnection',
    install_requires=install_requires,
    packages=find_packages(exclude=['tests'])
)
