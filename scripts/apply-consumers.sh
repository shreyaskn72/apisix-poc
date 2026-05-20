
#!/bin/bash

ADMIN_API_KEY="edd1c9f034335f136f87ad84b625c8f1"

echo "Applying clientA consumer..."

curl http://127.0.0.1:9180/apisix/admin/consumers/clientA \
  -X PUT \
  -H "X-API-KEY: ${ADMIN_API_KEY}" \
  -d @../consumers/clientA.json

echo ""
echo "Applying clientB consumer..."

curl http://127.0.0.1:9180/apisix/admin/consumers/clientB \
  -X PUT \
  -H "X-API-KEY: ${ADMIN_API_KEY}" \
  -d @../consumers/clientB.json

echo ""
echo "Consumers applied successfully."
