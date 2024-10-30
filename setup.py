from setuptools import setup

setup(
    version="3.0.0",
    name="dcm-import-module",
    description="flask app for import-module-containers",
    author="LZV.nrw",
    install_requires=[
        "flask==3.*",
        "PyYAML==6.*",
        "requests==2.*",
        "data-plumber-http>=1.0.0,<2",
        "dcm-common[services, db, orchestration]>=3.14.0,<4",
        "oai-pmh-extractor>=3.0.0,<4.0.0",
        "dcm-import-module-api>=5.2.0,<6",
        "dcm-ip-builder-sdk>=3.1.0,<4.0.0",
    ],
    packages=[
        "dcm_import_module",
        "dcm_import_module.plugins",
        "dcm_import_module.models",
        "dcm_import_module.views",
    ],
    extras_require={
        "cors": ["Flask-CORS==4"],
    },
    setuptools_git_versioning={
          "enabled": True,
          "version_file": "VERSION",
          "count_commits_from_version_file": True,
          "dev_template": "{tag}.dev{ccount}",
    },
)