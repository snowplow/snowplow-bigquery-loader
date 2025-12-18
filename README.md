# Snowplow Bigquery Loader

[![Build Status][build-image]][build]
[![Release][release-image]][releases]
[![License][license-image]][license]

## Introduction

This project contains applications required to load Snowplow data into Bigquery with low latency.

Check out [the example config files](./config) for how to configure your loader.

#### Azure

The Azure bigquery loader reads the stream of enriched events from Event Hubs.

Basic usage:
`
```bash
docker run \
  -v /path/to/config.hocon:/var/config.hocon \
  snowplow/bigquery-loader-kafka:2.1.0 \
  --config /var/config.hocon \
  --iglu-config /var/iglu.json
```

#### GCP

The GCP bigquery loader reads the stream of enriched events from Pubsub.

```bash
docker run \
  -v /path/to/config.hocon:/var/config.hocon \
  snowplow/bigquery-loader-pubsub:2.1.0 \
  --config /var/config.hocon \
  --iglu-config /var/iglu.json
```

#### AWS

The AWS bigquery loader reads the stream of enriched events from Kinesis.

```bash
docker run \
  -v /path/to/config.hocon:/var/config.hocon \
  snowplow/bigquery-loader-kinesis:2.1.0 \
  --config /var/config.hocon \
  --iglu-config /var/iglu.json
```

## Find out more

| Technical Docs             | Setup Guide          | Roadmap & Contributing |
|----------------------------|----------------------|------------------------|
| ![i1][techdocs-image]      | ![i2][setup-image]   | ![i3][roadmap-image]   |
| [Technical Docs][techdocs] | [Setup Guide][setup] | [Roadmap][roadmap]     |



## Copyright and License

Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.

Licensed under the [Snowplow Limited Use License Agreement][license]. _(If you are uncertain how it applies to your use case, check our answers to [frequently asked questions][faq].)_

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[setup]: https://docs.snowplow.io/docs/getting-started-on-snowplow-open-source/
[techdocs]: https://docs.snowplow.io/docs/pipeline-components-and-applications/loaders-storage-targets/bigquery-loader/
[roadmap]: https://github.com/snowplow/snowplow/projects/7

[build-image]: https://github.com/snowplow-incubator/snowplow-bigquery-loader/workflows/CI/badge.svg
[build]: https://github.com/snowplow-incubator/snowplow-bigquery-loader/actions/workflows/ci.yml

[release-image]: https://img.shields.io/badge/release-2.1.0-blue.svg?style=flat
[releases]: https://github.com/snowplow-incubator/snowplow-biguery-loader/releases

[license]: https://docs.snowplow.io/limited-use-license-1.1
[license-image]: https://img.shields.io/badge/license-Snowplow--Limited--Use-blue.svg?style=flat
