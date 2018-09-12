# prometheus-adapter

The prometheus-adapter is a remote storage adapter for Prometheus. It implements the read/write Prometheus remote API which acts as a backend to Prometheus for storing data long term in KairosDB. 


#### Endpoints

| Endpoint                       | Description                                                              |
|--------------------------------|--------------------------------------------------------------------------|
| api/v1/prometheus/writeadapter | Listens to metrics from Prometheus and pushes them to KariosDB.          |
| api/v1/prometheus/readadapter  | Translates a Prometheus query into a KairosDB query and returns the data.|

### Write Adapter 
The write adapter listens for Prometheus time series data, parses it, and writes it to KairosDB. 
The label named "\_\_name\_\_" is used as the metric name. All other labels are translated into KairosDB tags.

These optional properties provide ways to manipulate or restrict the data written to KairosDB.


| Property                       | Description                                                             |
|--------------------------------|-------------------------------------------------------------------------|
| kairosdb.plugin.prometheus-adapter.writer.prefix      | Prefix prepended to each metric name. |
| kairosdb.plugin.prometheus-adapter.writer.dropMetrics | This is a comma delimited list of regular expressions. Metric names that match any of the regular expressions are ignored and not added to KairosDB. | 
| kairosdb.plugin.prometheus-adapter.writer.dropLabels   | This is a comma delimited list of regular expressions. Labels (except for "\_\_name\_\_") that match any of the expressions are not included in metrics written to KairosDB. |
