from setuptools import find_packages, setup

requirements = ["kafka-python",
                "boto3"]

setup(name="customKafka", version="0.0.1",
      url='https://github.com/Ourinvest/common-kafka.git',
      packages=find_packages(),
      install_requires=requirements)
