from setuptools import setup

setup(
    name="cuttlefish-api",
    packages=["cuttlefish_api"],
    version="0.0.2",
    description="Cuttlefish API to integrate frontend and backend.",
    author="Kevin McAreavey",
    author_email="kevin.mcareavey@bristol.ac.uk",
    license="Protected",
    install_requires=[
        "bjoern",
        "dacite",
        "falcon",
        "falcon_auth",
        "pendulum",
    ],
    extras_require={},
    classifiers=[],
    include_package_data=True,
    platforms="any",
    entry_points={
        "console_scripts": ["cuttlefish-api=cuttlefish_api.main:cli"],
    },
)
