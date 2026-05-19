
#!/bin/bash

ADMIN_API_KEY="edd1c9f034335f136f87ad84b625c8f1"

echo "Applying APISIX route..."

curl http://127.0.0.1:9180/apisix/admin/routes/1 \
  -X PUT \
  -H "X-API-KEY: ${ADMIN_API_KEY}" \
  -d @../routes/trigger-route.json

echo ""
echo "Route applied successfully."

