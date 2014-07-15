# -*- coding: utf-8 -*-
from setuptools import setup, find_packages
import xml.sax.saxutils
from os.path import join
import sys
import os
from six import text_type

def get_version(fname='dabroker/__init__.py'):
    with open(fname) as f:
        for line in f:
            if line.startswith('__VERSION__'):
                return eval(line.split('=')[-1])

try:
    from msgfmt import Msgfmt
except:
    sys.path.insert(0, join(os.getcwd(), 'formalchemy'))

def compile_po(path):
    from msgfmt import Msgfmt
    for language in os.listdir(path):
        lc_path = join(path, language, 'LC_MESSAGES')
        if os.path.isdir(lc_path):
            for domain_file in os.listdir(lc_path):
                if domain_file.endswith('.po'):
                    file_path = join(lc_path, domain_file)
                    mo_file = join(lc_path, '%s.mo' % domain_file[:-3])
                    mo_content = Msgfmt(file_path, name=file_path).get()
                    mo = open(mo_file, 'wb')
                    mo.write(mo_content)
                    mo.close()

try:
    compile_po(join(os.getcwd(), 'formalchemy', 'i18n_resources'))
except Exception:
    print('Error while building .mo files')
else:
    print('.mo files compilation success')

def read(filename):
    text = open(filename,'r').read()
    return xml.sax.saxutils.escape(text)

long_description = read('README.rst')

REQUIREMENTS = [i.strip() for i in open("requirements.txt").readlines()
                                if not i.startswith("http")]

setup(name='DaBroker',
      license='GPLv3+',
      version='.'.join(str(x) for x in get_version()),
      description='DaBroker is a fast distributed object broker for large, mostly-read-only data',
      long_description=long_description,
      author='Matthias Urlichs',
      author_email='matthias@urlichs.de',
      url='http://dabroker.n-online.de/',
      install_requires=REQUIREMENTS,
      packages=find_packages(exclude=('tests',)),
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python',
          'Topic :: Database :: Front-Ends',
          'Topic :: Software Development :: Libraries :: Application Frameworks',
          'Topic :: Software Development :: Libraries :: Python Modules',
          'Topic :: Utilities',
      ],
      zip_safe=True,

      )

