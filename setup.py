from setuptools import setup

setup(
    version="4.5.0",
    name="dcm-import-module",
    description="flask app implementing the DCM Import Module API",
    author="LZV.nrw",
    license="MIT",
    python_requires=">=3.10",
    install_requires=[
        "flask==3.*",
        "PyYAML==6.*",
        "requests==2.*",
        "data-plumber-http>=1.0.0,<2",
        "dcm-common[services, db, orchestration]>=3.28.0,<4",
        "oai-pmh-extractor>=3.4.0,<4",
        "dcm-import-module-api>=6.1.0,<7",
        "dcm-ip-builder-sdk>=4.0.0,<6",
        "dcm-object-validator-sdk>=5.0.0,<6",
    ],
    packages=[
        "dcm_import_module",
        "dcm_import_module.components",
        "dcm_import_module.components.adapters",
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
