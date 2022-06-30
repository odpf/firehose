module.exports = {
  docsSidebar: [
    'introduction',
    {
      type: "category",
      label: "Guides",
      collapsed: false,
      items: [
        "guides/create_firehose",
        "guides/json-based-filters",
        "guides/jexl-based-filters",
        "guides/deployment",
        "guides/manage",
      ],
    },
    {
      type: "category",
      label: "Sinks",
      collapsed: false,
      items: [
        "sinks/http-sink",
        "sinks/grpc-sink",
        "sinks/jdbc-sink",
        "sinks/bigquery-sink",
        "sinks/influxdb-sink",
        "sinks/prometheus-sink",
        "sinks/mongo-sink",
        "sinks/redis-sink",
        "sinks/elasticsearch-sink",
        "sinks/blob-sink",
      ],
    },
    {
      type: "category",
      label: "Concepts",
      items: [
        "concepts/overview",
        "concepts/architecture",
        "concepts/filters",
        "concepts/templating",
        "concepts/consumer",
        "concepts/decorators",
        "concepts/offsets",
        "concepts/monitoring",
      ],
    },
    {
      type: "category",
      label: "Advance",
      items: [
        "advance/generic",
        "advance/errors",
        "advance/dlq",
        "advance/filters",
        "advance/retries",
        "advance/sink-pool",
      ],
    },
    {
      type: "category",
      label: "Reference",
      items: [
        "reference/metrics",
        "reference/core-faqs",
        "reference/faq",
        "reference/glossary",
      ],
    },
    {
      type: "category",
      label: "Contribute",
      items: [
        "contribute/contribution",
        "contribute/development",
      ],
    },
    'roadmap',
  ],
};