from setuptools import setup


setup(
	name="weasyl-smallcopy",
	version="0.1.0",
	url="https://github.com/Weasyl/smallcopy",
	license="ISC",
	classifiers=[
		"Programming Language :: Python :: 3",
		"Programming Language :: Python :: 3.6",
	],
	packages=[
		"weasyl_smallcopy",
	],
	install_requires=[
		"psycopg2==2.7.1",
	],
)
