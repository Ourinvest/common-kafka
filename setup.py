from setuptools import find_packages, setup

requirements = ["aiokafka"]

setup(name="common-kafka", version="1.0.0",
      packages=find_packages(), install_requires=requirements)
