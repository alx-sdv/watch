from setuptools import setup
setup(name='watch'
      , version='0.0.1'
      , description='Oracle Database monitoring'
      , author='alx-sdv'
      , url='https://github.com/alx-sdv/watch'
      , license='MIT'
      , python_requires='>=3.6.0'
      , include_package_data=True
      , packages=['watch']
      , install_requires=['cx-Oracle>=6.0.3', 'Flask>=0.12.2', 'pygal>=2.4.0']
      , scripts=['bin/run_watch.py'])
