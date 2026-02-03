#!/usr/bin/env bash
set -euo pipefail

# Shows the IP/domain for each service in the infrastructure

CLUSTER="AgenticHarnessCluster"

echo "Services:"
echo ""

# Tracker - has a domain
echo "Tracker:"
echo "  https://benchmark-tracker.vals.ai"
echo ""

# Swebench - get task public IP
echo "Swebench:"
TASK_ARN=$(aws ecs list-tasks --cluster "$CLUSTER" --service-name Swebench --query 'taskArns[0]' --output text 2>/dev/null)

if [[ -z "$TASK_ARN" || "$TASK_ARN" == "None" ]]; then
    echo "  (no running tasks)"
else
    ENI_ID=$(aws ecs describe-tasks --cluster "$CLUSTER" --tasks "$TASK_ARN" \
        --query 'tasks[0].attachments[0].details[?name==`networkInterfaceId`].value' --output text 2>/dev/null)

    if [[ -n "$ENI_ID" && "$ENI_ID" != "None" ]]; then
        PUBLIC_IP=$(aws ec2 describe-network-interfaces --network-interface-ids "$ENI_ID" \
            --query 'NetworkInterfaces[0].Association.PublicIp' --output text 2>/dev/null)

        if [[ -n "$PUBLIC_IP" && "$PUBLIC_IP" != "None" ]]; then
            echo "  http://${PUBLIC_IP}:8000"
        else
            echo "  (no public IP)"
        fi
    else
        echo "  (could not get ENI)"
    fi
fi
