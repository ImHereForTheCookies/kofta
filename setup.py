from setuptools import setup, find_packages

setup(name="kufta",
      version="0.0.1",
      author="Robert Meyer",
      py_modules=['kufta'],
      packages=["kufta"],
      install_requires=['confluent_kafka', 'tqdm'],
      )
