# AI-Ops: Data collector microservice

[![Build Status](https://travis-ci.org/ManageIQ/aiops-data-collector.svg?branch=master)](https://travis-ci.org/ManageIQ/aiops-data-collector)
[![codecov](https://codecov.io/gh/ManageIQ/aiops-data-collector/branch/master/graph/badge.svg)](https://codecov.io/gh/ManageIQ/aiops-data-collector)
[![License](https://img.shields.io/badge/license-APACHE2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

Thin Flask server collecting data from S3 on demand and streaming it through the pipeline

## Get Started

* Learn about other services within our pipeline
  - [incoming-listener](https://github.com/ManageIQ/aiops-incoming-listener)
  - [data-collector](https://github.com/ManageIQ/aiops-data-collector)
  - [publisher](https://github.com/ManageIQ/aiops-publisher)
* Discover all AI services we're integrating with
  - [dummy-ai](https://github.com/ManageIQ/aiops-dummy-ai-service)
  - [aicoe-insights-clustering](https://github.com/RedHatInsights/aicoe-insights-clustering)
* See deployment templates in the [e2e-deploy](https://github.com/RedHatInsights/e2e-deploy) repository

## Configure

* `AWS_ACCESS_KEY_ID` - AWS S3 credentials
* `AWS_SECRET_ACCESS_KEY` - AWS S3 credentials
* `AWS_S3_BUCKET_NAME` - AWS S3 bucket selector
* `NEXT_MICROSERVICE_HOST` - where to pass the collected data (`hostname:port`)

## License

See [LICENSE](LICENSE)
