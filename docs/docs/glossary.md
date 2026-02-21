# Glossary

## Source

A project object that loads dataset rows for metric computation.

## Metric

A `MetricSpec` definition binding metric ID, source ID, compute function, and output schema.

## Target

A project object that persists and retrieves metric rows for a metric/source pair.

## Config root

The project directory containing `sources/`, `metrics/`, and `targets/` modules.

## Metric run

One execution of a metric spec against a source and target, including trace metadata.

## Existing metrics

Previously stored metric rows loaded from a target and passed into a metric compute function.

## Deployment mode

UI launch mode using injected runtime paths and optional project discovery metadata.

## Demo mode

UI launch mode using package fixture data from `acme_metrics.demo`.
