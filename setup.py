from distutils.core import setup

setup(
    name='koalacore',
    packages=['koalacore'],  # this must be the same as the name above
    version='0.1.5-alpha',
    description='Tools for writing APIs on Google App Engine. You *must* install the GAE SDK for this package to work.',
    author='Matt Badger',
    author_email='foss@lighthouseuk.net',
    url='https://github.com/LighthouseUK/koalacore',  # use the URL to the github repo
    download_url='https://github.com/LighthouseUK/koalacore/tarball/0.1.5-alpha',  # I'll explain this in a second
    keywords=['gae', 'lighthouse'],  # arbitrary keywords
    classifiers=[],
    requires=['six', 'blinker']
)
