# Sample

This sample is for trying AFM locally without Kubernetes. 
It shows a server configured to serve [The New York City taxi trip record data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

## Steps

1. Run the server with
    ```bash
    pipenv run server --config sample/sample.yaml
    ```
1. Run a sample client with
    ```bash
    pipenv run python sample/sample.py
    ```
