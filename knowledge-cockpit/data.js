window.KNOWLEDGE_COCKPIT_DATA = {
  repoName: "Data Engineering PoCs",
  summary:
    "A presenter cockpit for the PoC narrative: 2 original project baselines, 2 runnable refactors and 5 platform extensions.",
  storyMap: {
    headline: "2 Original + 2 Refactor + 5 Extension",
    intro:
      "The live demo starts from two real project baselines, expands them into two runnable PoC refactors, then uses five extensions to show the complete data-platform lifecycle.",
    groups: [
      {
        id: "original",
        label: "2 Original",
        number: "2",
        title: "Actual Project Baseline",
        summary:
          "The original work was intentionally practical: understand the business process, clean the data, define the model and support the decision workflow.",
        defaultOpen: true,
        items: [
          {
            id: "original-oee",
            title: "OEE Data Automation",
            eyebrow: "Manufacturing analytics baseline",
            oneLiner:
              "A factory OEE workflow built around API and Excel sources, field normalization, shift logic and Tableau-ready datasets.",
            evidence: [
              "8 OEE API links plus 2 machine-status API links",
              "OEE raw fields standardized from about 23 to 24 fields",
              "Status raw fields standardized from about 8 to 10 fields",
              "Machine master joins for plant, process and machine mapping",
              "Data volume was moderate; complexity came from business logic, not Spark-scale volume"
            ],
            talkTrack:
              "The original OEE project is the business anchor: inconsistent source fields, cross-day shift calculation, status duration, deduplication and dashboard-ready output.",
            repoRef: "oee-data-platform/README.md",
            anchorTermId: "olap"
          },
          {
            id: "original-cce",
            title: "Customer Campaign Engine",
            eyebrow: "Campaign decisioning baseline",
            oneLiner:
              "A customer campaign data flow from CAS to Databricks EDL, CDP and Adobe Journey Optimizer, centered on identity and eligibility.",
            evidence: [
              "Three-layer segmentation framework: Identification, Classification and Value Stream",
              "Unified identity key across NRIC, FIN and Passport identifiers",
              "Downstream features and campaign rules require consistent customer identity",
              "Maps naturally to Bronze raw data, Silver identity resolution and Gold eligibility features"
            ],
            talkTrack:
              "The original CCE work is the decisioning anchor: resolve customer identity, organize segmentation logic and make campaign eligibility calculable downstream.",
            repoRef: "cce-feature-platform/README.md",
            anchorTermId: "real-time-vs-big-data"
          }
        ]
      },
      {
        id: "refactor",
        label: "2 Refactor",
        number: "2",
        title: "Runnable PoC Refactors",
        summary:
          "The refactors turn project experience into executable repositories with local demos, tests, docs and production mapping.",
        defaultOpen: true,
        items: [
          {
            id: "refactor-oee",
            title: "OEE Data Platform Refactor",
            eyebrow: "From dashboard enablement to governed analytics",
            oneLiner:
              "The OEE refactor keeps the real manufacturing logic but packages ingestion, normalization, quality checks and analytics as a runnable data platform.",
            evidence: [
              "Local ETL and analytics workflow",
              "Factory field normalization and OEE metric calculation",
              "Dashboard-ready outputs and anomaly-oriented analytical thinking",
              "Clear upgrade path from local Python/SQL to managed data platforms"
            ],
            talkTrack:
              "This is not pretending the original was big data. It shows the right-sized engineering choice first, then documents how it could scale if the operating context changes.",
            repoRef: "oee-data-platform/README.md",
            anchorTermId: "olap"
          },
          {
            id: "refactor-cce",
            title: "CCE Feature Platform Refactor",
            eyebrow: "From segmentation architecture to online features",
            oneLiner:
              "The CCE refactor turns campaign segmentation into identity resolution, feature engineering, model scoring and low-latency feature serving.",
            evidence: [
              "Identity resolution and feature snapshots",
              "Local online store mapped to Redis in production",
              "Real-time and batch paths converge at the Feature API",
              "480K active-user sizing and deployment artifacts"
            ],
            talkTrack:
              "The refactor keeps the CCE business idea but exposes the platform shape: Gold features for correctness, Redis/API for latency and docs for production sizing.",
            repoRef: "cce-feature-platform/README.md",
            anchorTermId: "real-time-vs-big-data"
          }
        ]
      },
      {
        id: "extension",
        label: "5 Extension",
        number: "5",
        title: "Platform Extensions",
        summary:
          "The extensions complete the platform story around transactions, governance, big data, real time and operations.",
        defaultOpen: true,
        items: [
          {
            id: "extension-oltp",
            title: "OMS OLTP Lifecycle",
            eyebrow: "Transactional hot path",
            oneLiner:
              "OMS and inventory flows explain current-state writes, reservation, Saga compensation, Outbox events and downstream handoff.",
            evidence: [
              "High-concurrency inventory reservation",
              "Redis/Lua fast path with database system of record",
              "Order, payment, inventory and fulfillment as distributed participants",
              "Commit and compensation paths are both explicit"
            ],
            talkTrack:
              "This extension makes the OLTP side concrete. It is the system that does the work before analytics can read the history.",
            repoRef: "inventory-oms-poc/README.md",
            anchorTermId: "oltp"
          },
          {
            id: "extension-governance",
            title: "Data Governance Checks",
            eyebrow: "Contract as monitoring",
            oneLiner:
              "The governance PoC turns contract ideas into schema, freshness, timestamp deviation, duplicate-event and reconciliation checks.",
            evidence: [
              "Executable checks instead of static policy language",
              "Timestamp and sequence validation for downstream OLAP trust",
              "Metrics output suitable for alerting and operational review",
              "Protects feature tables and dashboards from silent drift"
            ],
            talkTrack:
              "Governance becomes useful when it behaves like SRE for data: detect, alert, reconcile and show the affected contract.",
            repoRef: "data-governance-poc/README.md",
            anchorTermId: "data-contract"
          },
          {
            id: "extension-big-data",
            title: "Big Data EMR/Delta Path",
            eyebrow: "Distributed batch and replay",
            oneLiner:
              "Spark generator and EMR/Delta job skeletons mirror the local Bronze, Silver and Gold pipeline in a distributed runtime.",
            evidence: [
              "Synthetic data generator for customers, identities, policies, transactions and CDC events",
              "Bronze ingest, Silver features, Gold segmentation and anomaly job skeletons",
              "Airflow/MWAA-style dependency and retry thinking",
              "S3/Delta layout, partitions, compaction and replay notes"
            ],
            talkTrack:
              "This extension shows where the same logic goes when data volume, replay windows and compute isolation require Spark and lakehouse storage.",
            repoRef: "cce-feature-platform/docs/BIG_DATA_EMR_DELTA_EXTENSION.md",
            anchorTermId: "bronze-silver-gold"
          },
          {
            id: "extension-realtime",
            title: "Real-Time Feature Serving",
            eyebrow: "Low-latency online path",
            oneLiner:
              "CDC, Kafka/MSK, stream processing, Redis and Feature API cover the path where fresh signals must be served quickly.",
            evidence: [
              "Batch builds trusted baselines; stream updates hot features",
              "Redis serves online reads while Delta preserves replayable history",
              "Feature API becomes the convergence point for campaign decisions",
              "Sizing notes explain the 480K active-user path"
            ],
            talkTrack:
              "The real-time path is not a replacement for big data. It is the low-latency companion to the replayable historical path.",
            repoRef: "cce-feature-platform/docs/REALTIME_FEATURE_PLATFORM_480K.md",
            anchorTermId: "real-time-vs-big-data"
          },
          {
            id: "extension-ops",
            title: "Operations, Cost And Troubleshooting",
            eyebrow: "Production maturity",
            oneLiner:
              "Operations separates OLTP latency, OLAP batch windows, IO, file layout, skew, lag, cache pressure and cost drivers.",
            evidence: [
              "Layered troubleshooting across API, database, Kafka, Spark, Delta/S3 and Redis",
              "Cost model for EKS, Redis, MSK, Spark/EMR, S3 and observability",
              "Maturity gap notes for coupled source systems and unclear ownership",
              "Distinguishes physical sharding from analytical partitioning"
            ],
            talkTrack:
              "This is the maturity layer: the design explains not only what to build, but how to keep it observable, debuggable and cost-aware.",
            repoRef: "cce-feature-platform/docs/OPERATIONS_MATURITY_AND_COST.md",
            anchorTermId: "operations"
          }
        ]
      }
    ]
  },
  terms: [
    {
      id: "oltp",
      title: "OLTP",
      category: "OLTP",
      tags: ["transactions", "ACID", "current state"],
      oneLiner:
        "OLTP is the system that does the business work: order placement, stock reservation, payment capture and cancellation.",
      explain:
        "In this repo, OLTP is represented by the OMS projects. The important properties are current mutable state, strict transaction boundaries, idempotent commands and low-latency point reads/writes.",
      repoRefs: [
        "README.md#oltp-vs-olap",
        "oms-oltp-poc/README.md",
        "inventory-oms-poc/README.md"
      ],
      related: ["olap", "outbox", "saga", "sharding"],
      talkTrack:
        "OLTP protects the current truth. It is optimized for live state transitions, not broad historical analysis."
    },
    {
      id: "olap",
      title: "OLAP",
      category: "OLAP",
      tags: ["analytics", "history", "warehouse"],
      oneLiner:
        "OLAP is the system that reads history: dashboards, trends, customer features, anomaly detection and decision support.",
      explain:
        "OLAP preserves how facts changed over time. It usually uses columnar or lakehouse storage, star/snowflake modeling, partitions, snapshots and slowly changing dimensions.",
      repoRefs: [
        "README.md#time-and-change-capture",
        "oee-data-platform/README.md",
        "cce-feature-platform/README.md"
      ],
      related: ["scd", "bronze-silver-gold", "warehouse-modeling", "data-contract"],
      talkTrack:
        "OLAP preserves the timeline of truth. The question is not only what the value is now, but what it was when the business event happened."
    },
    {
      id: "outbox",
      title: "Outbox Pattern",
      category: "Integration",
      tags: ["events", "reliability", "CDC"],
      oneLiner:
        "Outbox persists business state and the outbound event in the same transaction so downstream systems can consume reliably.",
      explain:
        "The OMS flow writes reservations/orders and emits events such as inventory.reserved or inventory.released. This bridges OLTP facts into Kafka/CDC and later OLAP pipelines.",
      repoRefs: [
        "oms-oltp-poc/README.md",
        "inventory-oms-poc/README.md#from-events-to-olap-contracts"
      ],
      related: ["cdc", "saga", "data-contract", "idempotency"],
      talkTrack:
        "Outbox is the handoff point: the business transaction commits first, then the event stream becomes the downstream timeline."
    },
    {
      id: "saga",
      title: "Saga Compensation",
      category: "OLTP",
      tags: ["workflow", "compensation", "inventory"],
      oneLiner:
        "Saga coordinates a distributed business flow with explicit commit and compensation paths.",
      explain:
        "For inventory, the happy path commits reserved stock after payment succeeds. The failure path releases inventory after payment failure, cancellation or timeout.",
      repoRefs: [
        "inventory-oms-poc/README.md#architecture-diagram",
        "oms-oltp-poc/tests/test_oms_flow.py"
      ],
      related: ["oltp", "outbox", "idempotency"],
      talkTrack:
        "Saga is how the system stays consistent when order, payment, inventory and fulfillment do not share one database transaction."
    },
    {
      id: "data-contract",
      title: "Executable Data Contract",
      category: "Governance",
      tags: ["schema", "freshness", "SRE"],
      oneLiner:
        "A data contract becomes real when it is enforced through checks, alerts and reconciliation.",
      explain:
        "The data-governance PoC checks schema drift, required payload fields, freshness, publish delay, duplicate semantic events, timestamp deviation and inventory reconciliation.",
      repoRefs: [
        "data-governance-poc/README.md",
        "data-governance-poc/contracts/oms_event_contract.json",
        "data-governance-poc/src/data_governance/monitor.py"
      ],
      related: ["olap", "cdc", "idempotency", "operations"],
      talkTrack:
        "The contract is not just documentation. The practical version is a set of checks that protects downstream dashboards and features."
    },
    {
      id: "bronze-silver-gold",
      title: "Bronze / Silver / Gold",
      category: "Big Data",
      tags: ["lakehouse", "ETL", "Delta"],
      oneLiner:
        "Bronze keeps raw history, Silver cleans and standardizes, Gold publishes analytics and feature outputs.",
      explain:
        "The CCE EMR/Delta extension maps local pipeline logic into distributed Spark jobs for Bronze ingest, Silver feature engineering, Gold segmentation and anomaly detection.",
      repoRefs: [
        "cce-feature-platform/docs/BIG_DATA_EMR_DELTA_EXTENSION.md",
        "cce-feature-platform/deploy/emr_delta/README.md"
      ],
      related: ["olap", "scd", "warehouse-modeling", "data-contract"],
      talkTrack:
        "Bronze/Silver/Gold separates source capture from clean facts and final products, which is what makes replay and governance practical."
    },
    {
      id: "warehouse-modeling",
      title: "Warehouse Modeling",
      category: "OLAP",
      tags: ["facts", "dimensions", "star schema"],
      oneLiner:
        "Warehouse modeling organizes historical facts and dimensions for analysis, not for OLTP request routing.",
      explain:
        "Fact tables, dimension tables, star schemas and snowflake schemas make business history easier to query. They are downstream of OLTP sharded tables and CDC/ETL.",
      repoRefs: [
        "README.md#sharding-vs-warehouse-modeling",
        "cce-feature-platform/docs/BIG_DATA_EMR_DELTA_EXTENSION.md#warehouse-layer-mapping"
      ],
      related: ["sharding", "partitioning", "scd", "olap"],
      talkTrack:
        "Sharding is a physical routing strategy. Warehouse modeling is a logical analytical strategy. They are not substitutes."
    },
    {
      id: "sharding",
      title: "OLTP Sharding",
      category: "OLTP",
      tags: ["scaling", "routing", "point lookup"],
      oneLiner:
        "Sharding spreads transactional writes and point reads across databases or tables using a stable routing key.",
      explain:
        "A shard key such as customer ID, order ID or phone hash tells the application where current operational records live. It is different from an OLAP partition key.",
      repoRefs: [
        "README.md#sharding-vs-warehouse-modeling",
        "cce-feature-platform/docs/OPERATIONS_MATURITY_AND_COST.md#layered-troubleshooting"
      ],
      related: ["partitioning", "oltp", "warehouse-modeling"],
      talkTrack:
        "If the problem is live transactional point lookup or write throughput, think sharding and caching before thinking warehouse modeling."
    },
    {
      id: "partitioning",
      title: "Partitioning By Layer",
      category: "Operations",
      tags: ["partition", "Kafka", "Delta"],
      oneLiner:
        "Partition means different things in OLTP tables, Kafka topics, OLAP lakehouse layouts and Delta/S3 file planning.",
      explain:
        "OLTP partitions reduce local scan range. Kafka partitions preserve per-key order and parallelism. OLAP partitions prune historical scans. Delta/S3 file layout controls small files and commit performance.",
      repoRefs: [
        "cce-feature-platform/docs/OPERATIONS_MATURITY_AND_COST.md#layered-troubleshooting",
        "cce-feature-platform/docs/BIG_DATA_EMR_DELTA_EXTENSION.md#spark--delta-design-points"
      ],
      related: ["sharding", "warehouse-modeling", "operations"],
      talkTrack:
        "Same word, different layer. Always ask: are we routing a request, preserving event order, pruning a scan or optimizing files?"
    },
    {
      id: "scd",
      title: "Slowly Changing Dimensions",
      category: "OLAP",
      tags: ["time", "history", "dimensions"],
      oneLiner:
        "SCD describes how dimension attributes preserve or overwrite change over time.",
      explain:
        "SCD Type 1 overwrites the latest value. SCD Type 2 keeps versions with effective date ranges so historical facts join to the correct dimension version.",
      repoRefs: ["README.md#time-and-change-capture"],
      related: ["olap", "warehouse-modeling", "idempotency"],
      talkTrack:
        "SCD is why time matters in OLAP. The customer segment at order time may differ from the customer segment today."
    },
    {
      id: "real-time-vs-big-data",
      title: "Real-Time vs Big-Data Path",
      category: "Real Time",
      tags: ["CDC", "Redis", "Spark"],
      oneLiner:
        "Big-data jobs build trusted historical baselines; real-time streams apply low-latency incremental updates.",
      explain:
        "In CCE, Spark/Delta/Airflow creates baseline features and segments. CDC/MSK/stream jobs update Redis for online serving. Feature API is the convergence point.",
      repoRefs: [
        "cce-feature-platform/docs/REALTIME_FEATURE_PLATFORM_480K.md",
        "cce-feature-platform/docs/OPERATIONS_MATURITY_AND_COST.md#real-time-and-big-data-relationship"
      ],
      related: ["cdc", "bronze-silver-gold", "operations"],
      talkTrack:
        "The two paths are complementary: batch gives correctness and replay, streaming gives low latency."
    },
    {
      id: "operations",
      title: "Operations And Troubleshooting",
      category: "Operations",
      tags: ["cost", "IO", "maturity"],
      oneLiner:
        "Operations separates CPU, memory, network, storage, IO, partition layout, lag and serving latency.",
      explain:
        "The operations note captures target state versus reality, IO impact, layered troubleshooting, cost drivers and maturity stages.",
      repoRefs: ["cce-feature-platform/docs/OPERATIONS_MATURITY_AND_COST.md"],
      related: ["partitioning", "real-time-vs-big-data", "data-contract"],
      talkTrack:
        "A mature design includes not only code and deployment, but also how to debug latency, lag, cost and data trust."
    }
  ],
  demoFlow: [
    {
      id: "original-two",
      title: "2 Original Projects",
      duration: "90 sec",
      goal: "Start from the actual project baselines before showing any refactor.",
      open: "README.md",
      talkingPoints: [
        "OEE was a practical factory analytics workflow: APIs, Excel master data, field normalization, shift logic and Tableau-ready datasets.",
        "CCE was a campaign decisioning workflow: CAS to Databricks EDL, CDP and Adobe Journey Optimizer, with identity and segmentation at the center.",
        "The original wording is intentionally simpler than the refactor: real projects first, platform interpretation second."
      ]
    },
    {
      id: "refactor-two",
      title: "2 Refactor Projects",
      duration: "2 min",
      goal: "Show how the two project baselines became runnable PoCs.",
      open: "README.md#projects",
      talkingPoints: [
        "OEE Data Platform packages ingestion, normalization, metric calculation and analytics into a runnable local project.",
        "CCE Feature Platform packages identity resolution, feature engineering, scoring, online store and API serving.",
        "The refactors preserve the business intent while making architecture, tests, docs and deployment shape explicit."
      ]
    },
    {
      id: "oltp-extension",
      title: "Extension 1: OMS OLTP Lifecycle",
      duration: "2 min",
      goal: "Explain the high-concurrency transactional side.",
      open: "inventory-oms-poc/README.md",
      image: "assets/high-concurrency-inventory-system-design.jpg",
      talkingPoints: [
        "Inventory reservation is the live transactional hot path.",
        "Redis/Lua protects high-concurrency reservation; DB remains system of record.",
        "Saga handles commit and compensation; Outbox/Kafka hands events to downstream systems."
      ]
    },
    {
      id: "governance-extension",
      title: "Extension 2: Data Governance",
      duration: "90 sec",
      goal: "Make governance concrete and operational.",
      open: "data-governance-poc/README.md",
      talkingPoints: [
        "The contract covers schema, payload shape, timestamp semantics, freshness and reconciliation.",
        "Checks output JSON or Prometheus-style metrics.",
        "The point is to protect OLAP tables and ML features from silent source drift."
      ]
    },
    {
      id: "big-data-extension",
      title: "Extension 3: Big Data EMR/Delta",
      duration: "2 min",
      goal: "Show how local pipeline logic maps to Spark/Delta/Airflow.",
      open: "cce-feature-platform/docs/BIG_DATA_EMR_DELTA_EXTENSION.md",
      talkingPoints: [
        "Local code remains runnable; EMR/Delta scripts show distributed execution.",
        "Bronze/Silver/Gold maps to raw history, cleaned detail and feature marts.",
        "Airflow/MWAA controls dependency order, retry behavior and SLA hooks."
      ]
    },
    {
      id: "realtime-extension",
      title: "Extension 4: Real-Time Feature Serving",
      duration: "2 min",
      goal: "Explain the relationship between batch correctness and online latency.",
      open: "cce-feature-platform/docs/REALTIME_FEATURE_PLATFORM_480K.md",
      talkingPoints: [
        "Spark/Delta/Airflow builds trusted baselines and replayable history.",
        "CDC/MSK/Redis updates hot features for low-latency campaign decisions.",
        "The Feature API is the convergence point between the two paths."
      ]
    },
    {
      id: "ops-extension",
      title: "Extension 5: Operations And Cost",
      duration: "2 min",
      goal: "Show maturity, cost and failure-mode thinking.",
      open: "cce-feature-platform/docs/OPERATIONS_MATURITY_AND_COST.md",
      talkingPoints: [
        "Real systems may be coupled and source semantics may be wrong.",
        "Troubleshooting separates QPS, slow query, connections, lag, IO and file layout.",
        "Cost is driven by Redis memory, MSK brokers, Spark input size, S3/Delta history and observability."
      ]
    }
  ],
  questions: [
    {
      id: "qps-vs-volume",
      question: "Does 1 billion rows mean high concurrency?",
      shortAnswer:
        "No. High concurrency is request rate. One billion rows is data volume, which can cause slow queries, IO pressure and connection pileups.",
      answer:
        "Separate QPS from data volume. A low-QPS query can still be dangerous if it scans too much data. Slow queries occupy connections, create queues and make the system look overloaded. The fix depends on the layer: indexing, sharding, partition pruning, caching or moving analytical scans out of OLTP."
    },
    {
      id: "sharding-vs-partition",
      question: "What is the difference between OLTP sharding and OLAP partitioning?",
      shortAnswer:
        "OLTP sharding routes current records for writes and point reads. OLAP partitioning prunes historical scans and controls replay scope.",
      answer:
        "A shard key tells the app where a live record lives. A warehouse partition key tells the engine which historical files or partitions to scan. A MySQL phone hash should not automatically become a Delta or ClickHouse partition key."
    },
    {
      id: "contract-abstract",
      question: "Is a data contract too abstract?",
      shortAnswer:
        "It is abstract only if it stays in a document. In this repo it becomes executable checks.",
      answer:
        "The data-governance PoC validates schema, payload fields, freshness, publish delay, duplicate semantic events, timestamp deviation and reconciliation. That is the operational version of a data contract."
    },
    {
      id: "unity-delta-contract",
      question: "Do Unity Catalog and Delta Lake naturally solve governance?",
      shortAnswer:
        "They provide strong infrastructure, but teams still define business meaning and quality rules.",
      answer:
        "Catalogs and Delta tables help with metadata, access control, lineage, ACID writes, schema enforcement and table history. They do not automatically define what event_time, customer_segment or available_stock mean."
    },
    {
      id: "realtime-bigdata",
      question: "What is the relationship between real-time and big-data paths?",
      shortAnswer:
        "Batch builds trusted baselines; streaming applies low-latency incremental changes.",
      answer:
        "Spark/Delta/Airflow is optimized for replay, full-history computation and feature windows. CDC/MSK/Redis is optimized for latency and online serving. The Feature API is where both paths converge."
    },
    {
      id: "scd-why",
      question: "Why does OLAP care so much about time and SCD?",
      shortAnswer:
        "Because analytics often needs the value as it was when the event happened, not only the latest value.",
      answer:
        "OLTP updates current state in place. OLAP must preserve transitions: order state changes, segment changes, inventory reservations and historical snapshots. SCD Type 2 lets historical facts join to the correct dimension version."
    }
  ],
  graph: [
    ["OLTP", "Outbox / CDC"],
    ["Outbox / CDC", "Data Governance"],
    ["Data Governance", "Bronze"],
    ["Bronze", "Silver"],
    ["Silver", "Gold"],
    ["Gold", "Feature API"],
    ["CDC / Stream", "Redis"],
    ["Redis", "Feature API"],
    ["Feature API", "Campaign Decisioning"],
    ["Operations", "Cost"],
    ["Operations", "Troubleshooting"],
    ["Troubleshooting", "Partitioning By Layer"]
  ]
};
