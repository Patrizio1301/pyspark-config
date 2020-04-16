DISTNAME = 'pyspark-config'
DESCRIPTION = 'A set of python modules for machine learning and data mining'
with open('README.rst') as f:
    LONG_DESCRIPTION = f.read()
MAINTAINER = 'Patrizio Guagliardo'
MAINTAINER_EMAIL = 'patrizio.guagliardo@gmx.de'
DOWNLOAD_URL = 'https://pypi.org/project/scikit-learn/#files'
LICENSE = 'new BSD'
PROJECT_URLS = {
    'Bug Tracker': 'https://github.com/scikit-learn/scikit-learn/issues',
    'Documentation': 'https://scikit-learn.org/stable/documentation.html',
    'Source Code': 'https://github.com/scikit-learn/scikit-learn'
}
VERSION=0.0.1

if platform.python_implementation() == 'PyPy':
    SCIPY_MIN_VERSION = '1.1.0'
    NUMPY_MIN_VERSION = '1.14.0'
else:
    SCIPY_MIN_VERSION = '0.19.1'
    NUMPY_MIN_VERSION = '1.13.3'

JOBLIB_MIN_VERSION = '0.11'
THREADPOOLCTL_MIN_VERSION = '2.0.0'


def setup_package():
    metadata = dict(name=DISTNAME,
                    maintainer=MAINTAINER,
                    maintainer_email=MAINTAINER_EMAIL,
                    description=DESCRIPTION,
                    license=LICENSE,
                    url=URL,
                    download_url=DOWNLOAD_URL,
                    project_urls=PROJECT_URLS,
                    version=VERSION,
                    long_description=LONG_DESCRIPTION,
                    classifiers=['Intended Audience :: Science/Research',
                                 'Intended Audience :: Developers',
                                 'License :: OSI Approved',
                                 'Programming Language :: C',
                                 'Programming Language :: Python',
                                 'Topic :: Software Development',
                                 'Topic :: Scientific/Engineering',
                                 'Operating System :: Microsoft :: Windows',
                                 'Operating System :: POSIX',
                                 'Operating System :: Unix',
                                 'Operating System :: MacOS',
                                 'Programming Language :: Python :: 3',
                                 'Programming Language :: Python :: 3.6',
                                 'Programming Language :: Python :: 3.7',
                                 'Programming Language :: Python :: 3.8',
                                 ('Programming Language :: Python :: '
                                  'Implementation :: CPython'),
                                 ('Programming Language :: Python :: '
                                  'Implementation :: PyPy')
                                 ],
                    cmdclass=cmdclass,
                    python_requires=">=3.6",
                    install_requires=[
                        'numpy>={}'.format(NUMPY_MIN_VERSION),
                        'scipy>={}'.format(SCIPY_MIN_VERSION),
                        'joblib>={}'.format(JOBLIB_MIN_VERSION),
                        'threadpoolctl>={}'.format(THREADPOOLCTL_MIN_VERSION)
                    ],
                    package_data={'': ['*.pxd']},
                    **extra_setuptools_args)

    if len(sys.argv) == 1 or (
            len(sys.argv) >= 2 and ('--help' in sys.argv[1:] or
                                    sys.argv[1] in ('--help-commands',
                                                    'egg_info',
                                                    'dist_info',
                                                    '--version',
                                                    'clean'))):
        # For these actions, NumPy is not required
        #
        # They are required to succeed without Numpy for example when
        # pip is used to install Scikit-learn when Numpy is not yet present in
        # the system.
        try:
            from setuptools import setup
        except ImportError:
            from distutils.core import setup

        metadata['version'] = VERSION
    else:
        if sys.version_info < (3, 6):
            raise RuntimeError(
                "Scikit-learn requires Python 3.6 or later. The current"
                " Python version is %s installed in %s."
                % (platform.python_version(), sys.executable))

        check_package_status('numpy', NUMPY_MIN_VERSION)

        check_package_status('scipy', SCIPY_MIN_VERSION)

        from numpy.distutils.core import setup

        metadata['configuration'] = configuration

    setup(**metadata)


if __name__ == "__main__":
    setup_package()
