# Bigtable Sink

Bigtable Sink is implemented in Firehose using the Bigtable sink connector implementation in ODPF Depot. You can check out ODPF Depot Github repository [here](https://github.com/odpf/depot).

### Configuration

For Bigtable sink in Firehose we need to set first (`SINK_TYPE`=`bigtable`). There are some generic configs which are common across different sink types which need to be set which are mentioned in [generic.md](../advance/generic.md). Bigtable sink specific configs are mentioned in ODPF Depot repository. You can check out the Bigtable Sink configs [here](https://github.com/odpf/depot/blob/main/docs/reference/configuration/bigtable.md)