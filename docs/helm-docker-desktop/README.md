# Grafeas-Elasticsearch in Docker for Desktop

This example deploys elasticsearch with minimal resources for local development.

```
helm repo add rode https://rode.github.io/charts
helm install grafeas-elasticsearch rode/grafeas-elasticsearch --values values.yaml
```