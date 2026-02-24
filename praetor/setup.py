from setuptools import setup

setup(name='praetor',
      version='4.11',
      python_requires='>=3.7',
      description='Automatic Generation of Provenance from Python3 Scripts',
      url='https://gitlab.mpcdf.mpg.de/PRAETOR/prov-PRAETOR_public/',
      author='Michael Johnson',
      author_email='michael.johnson0100@gmail.com',
      license='MIT',
      packages=['praetor'],
      install_requires=['pandas', 'prov', 'requests'],
      scripts=["bin/create_ttl.py"],
      zip_safe=False)

