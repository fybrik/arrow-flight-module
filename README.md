[![GitHub Actions Build](https://github.com/fybrik/arrow-flight-module/actions/workflows/build.yml/badge.svg)](https://github.com/fybrik/arrow-flight-module/actions/workflows/build.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# arrow-flight-module

The arrow-flight-module (AFM) for [Fybrik](https://github.com/fybrik/fybrik) brings enforcement of data governance policies to the world of [Apache Arrow](https://arrow.apache.org/)
[Flight](https://arrow.apache.org/docs/format/Flight.html).

## What is it? 

AFM is a Flight server that enables applications to consume tabular data from data sources. More importantly, the AFM is also a Policy Enforcement Point (PEP) capable of performing enforcement actions dictated by a Policy Decision Point.
Such enforcement actions include blocking unauthorized requests, validating and transforming data payloads. 

## Register as a Fybrik module

To register AFM as a Fybrik module apply `module.yaml` to the fybrik-system namespace of your cluster.

To install the latest release run:

```bash
kubectl apply -f https://github.com/fybrik/arrow-flight-module/releases/latest/download/module.yaml -n fybrik-system
```

### Version compatibility matrix

| Fybrik           | AFM     | Command
| ---              | ---     | ---
| 0.1.x            | 0.1.x   | `https://github.com/fybrik/arrow-flight-module/releases/download/v0.1.0/module.yaml`
| 0.2.x            | 0.2.x   | `https://github.com/fybrik/arrow-flight-module/releases/download/v0.2.0/module.yaml`
| 0.3.x            | 0.3.x   | `https://github.com/fybrik/arrow-flight-module/releases/download/v0.3.0/module.yaml`
| 0.4.x            | 0.4.x   | `https://github.com/fybrik/arrow-flight-module/releases/download/v0.4.0/module.yaml`
| 0.5.x            | 0.5.x   | `https://github.com/fybrik/arrow-flight-module/releases/download/v0.5.0/module.yaml`
| 0.6.x            | 0.6.x   | `https://github.com/fybrik/arrow-flight-module/releases/download/v0.6.0/module.yaml`
| 0.7.x            | 0.7.x   | `https://github.com/fybrik/arrow-flight-module/releases/download/v0.7.0/module.yaml`
| 1.0.x            | 0.8.x   | `https://github.com/fybrik/arrow-flight-module/releases/download/v0.8.0/module.yaml`
| 1.1.x            | 0.8.x   | `https://github.com/fybrik/arrow-flight-module/releases/download/v0.8.0/module.yaml`
| 1.2.x            | 0.9.x   | `https://github.com/fybrik/arrow-flight-module/releases/download/v0.9.0/module.yaml`
| 1.2.x            | 0.10.x  | `https://github.com/fybrik/arrow-flight-module/releases/download/v0.10.0/module.yaml`
| master           | master  | `https://raw.githubusercontent.com/fybrik/arrow-flight-module/master/module.yaml`


## Build and deploy to Kubernetes

These instructions are for building an image of AFM and 
deploying it to Kubernetes. Deployment will install a Helm release named `afm` to the cluster. The release runs the flight server with **empty configuration**.


### Requirements

- make
- Docker
- kubectl with access to a kubernetes cluster (e.g., a [kind](https://kind.sigs.k8s.io/) cluster)
- [Helm 3.7](https://helm.sh/docs/intro/install/) or above

### Deploy to kind clusters

```bash
make build push-to-kind deploy
```

### Deploy to other clusters

For other clusters you will need to use an image registry. 
For example if your image registry is `us.icr.io/username` then use:
```bash
REPOSITORY=us.icr.io/username/arrow-flight-module make build push deploy
```

### Configuration

You can provide a configuration file by upgrading the deployed Helm chart.
For example, to use `sample/sample.yaml` as configuration run:

```bash
helm upgrade --reuse-values --set-file config_override=sample/sample.yaml afm ./helm/afm
```

It is also possible to specify specific configuration values following the modules specification of Fybrik. Generally, you would want Fybrik to do that for you and not deal with it directly.

## Usage

Once the server is deployed you can connect to it using any Flight client SDK.
For example, in python:

```python
import pyarrow.flight as fl
import pandas as pd

if __name__ == '__main__':
  client = fl.connect("grpc://afm-arrow-flight-module.default.svc.cluster.local:80")  # change to the address that the server is deployed to
  info: fl.FlightInfo = client.get_flight_info(
      fl.FlightDescriptor.for_command(r'{"asset": "sample.parquet"}'))  # change to an asset configured in the server config file
  result: fl.FlightStreamReader = client.do_get(info.endpoints[0].ticket)

  df: pd.DataFrame = result.read_pandas()
  print(df)
```

## Development

This project requires Python 3.8 and pipenv. 

Run the server locally with `pipenv install` and then `pipenv run server`.

See `/sample` for an example to run locally.

## Status

This project is in a **very early** stage and contains just the following features:

- [X] Formats
  - [X] Parquet
- [X] Filesystems
  - [X] S3  
- [X] Queries
  - [X] Asset name
  - [X] Column Selection
- [X] Enforcement Actions
  - [X] Redact
  - [X] RemoveColumn
- [X] Configuration (assets, actions)
- [X] Demo
  - [X] Flight client


The project focus is to transform data based on policies. 
We are planning to create a specialized architecture for a generic PEP
for Apache Arrow Flight and demonstrating it with an existing flight server 
such as [Ballista](https://github.com/ballista-compute/ballista).

A full list of deisred features (not necessarily fulfilled by AFM alone):

- [ ] Formats
  - [X] Parquet
  - [ ] Parquet Modular Encryption (PME)
  - [ ] CSV
  - [ ] JSON
  - [ ] ORC
  - [ ] Arrow Flight
- [ ] Filesystems
  - [X] S3  
  - [ ] Local
- [ ] Queries
  - [X] Asset name
  - [X] Column selection
  - [ ] Nested column selection
  - [ ] Filter
  - [ ] Full SQL
- [ ] Enforcement Actions
  - [ ] Redact
  - [ ] RemoveColumn
  - [ ] Masking (format preserving)
  - [ ] Blackout period
- [ ] Plugable enforcement actions
- [ ] Simplified dynamic configuration (personal asset catalog, action policies)
- [ ] Distributed query planning
- [ ] Serverless query execution
- [ ] Support writes
- [ ] Integrations
  - [ ] [dataset-lifecycle-framework](https://github.com/IBM/dataset-lifecycle-framework)
- [ ] Demo
  - [ ] Flight client
  - [ ] Apache Spark client
