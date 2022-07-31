# Release Process

The process of creating a release is described in this document. Replace `X.Y.Z` with the version to be released.

## 1. Create a [new release](https://github.com/fybrik/arrow-flight-module/releases/new) 

- Use `vX.Y.Z` tag and set `master` as the target.
- Update the tags `spec.chart.values.image.tag` and `spec.chart.name` in module.yaml file to be `X.Y.Z` and attach the file to the release.
- Update `Version compatibility matrix` section in README.md if needed.

Ensure that the release notes explicitly mention upgrade instructions and any breaking change.
