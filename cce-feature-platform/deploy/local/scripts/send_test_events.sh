#!/bin/bash
# Send test CDC events to Kafka

set -e

TOPIC="cce.rds.orders"
BOOTSTRAP_SERVER="localhost:9092"

echo "Sending test CDC events to $TOPIC..."

# Sample CDC event (Debezium format)
cat <<EOF | docker exec -i cce-kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic $TOPIC \
  --property "parse.key=true" \
  --property "key.separator=|"
evt_001|{"schema":null,"payload":{"before":null,"after":{"order_id":"O-1001","unified_customer_key":"U0001","id_type":"NRIC","id_value":"S1234567A","amount":288.0,"product":"INSURANCE"},"source":{"version":"2.1.0","connector":"mysql","name":"cce-db","ts_ms":1725350400000,"snapshot":"false","db":"cce","table":"orders","server_id":1,"gtid":null,"file":"mysql-bin.000001","pos":12345,"row":0},"op":"c","ts_ms":1725350400123,"transaction":null}}
evt_002|{"schema":null,"payload":{"before":null,"after":{"order_id":"O-1002","unified_customer_key":"U0002","id_type":"FIN","id_value":"G7654321K","amount":420.0,"product":"INVESTMENT"},"source":{"version":"2.1.0","connector":"mysql","name":"cce-db","ts_ms":1725350408000,"snapshot":"false","db":"cce","table":"orders","server_id":1,"gtid":null,"file":"mysql-bin.000001","pos":12456,"row":0},"op":"c","ts_ms":1725350408234,"transaction":null}}
evt_003|{"schema":null,"payload":{"before":null,"after":{"order_id":"O-1003","unified_customer_key":"U0001","id_type":"NRIC","id_value":"S1234567A","amount":150.0,"product":"SAVINGS"},"source":{"version":"2.1.0","connector":"mysql","name":"cce-db","ts_ms":1725350415000,"snapshot":"false","db":"cce","table":"orders","server_id":1,"gtid":null,"file":"mysql-bin.000001","pos":12567,"row":0},"op":"c","ts_ms":1725350415345,"transaction":null}}
EOF

echo ""
echo "Sent 3 test events!"
echo ""
echo "Check consumer:"
echo "  docker exec cce-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic $TOPIC --from-beginning --max-messages 3"
