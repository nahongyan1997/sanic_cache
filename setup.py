from setuptools import setup

setup(
    name='sanic_cache',
    version='0.1.2',
    packages=['sanic_cache'],
    url='https://github.com/nahongyan1997/sanic_cache',
    license='MIT',
    author='Na haha',
    author_email='547840062@qq.com',
    description='flask-cache -> sanic-cache',
    install_requires=[
        "sanic >= 23.12.1",
        "sanic_ext",
        "redis >= 5.0.0",
    ]
)
