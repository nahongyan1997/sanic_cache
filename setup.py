from setuptools import setup, find_packages

setup(
    name='sanic_cache',
    version='0.1.0',
    packages=["sanic_cache"],
    url='https://github.com/nahongyan1997/sanic_cache',
    license='MIT',
    author='Na Haha',
    author_email='547840062@qq.com',
    description='flask-cache -> sanic-cache',
    install_requires = [
        'sanic',
        "sanic_ext",
        "redis-py"
    ],
)
