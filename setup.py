# Automatically created by: gerapy
from setuptools import setup, find_packages
setup(
    name='NewsSpider',
    version='1.0',
    packages=find_packages(),
    entry_points={'scrapy':['settings=NewsSpider.settings']},
)