#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<EOF
Usage: ./whitelist-ip.sh <command> [args]

Commands:
  --add [IP] [DESC]     Add IP to whitelist (auto-detects IP if omitted)
  --remove [IP]         Remove IP from whitelist (auto-detects IP if omitted)
  --list                List all whitelisted IPs

Examples:
  ./whitelist-ip.sh --add                    # Add current IP
  ./whitelist-ip.sh --add 1.2.3.4 "Home"     # Add specific IP with description
  ./whitelist-ip.sh --remove                 # Remove current IP
  ./whitelist-ip.sh --remove 1.2.3.4         # Remove specific IP
  ./whitelist-ip.sh --list                   # List all whitelisted IPs
EOF
    exit 0
}

ACTION=""
case "${1:-}" in
    --add)    ACTION="add"; shift ;;
    --remove) ACTION="remove"; shift ;;
    --list)   ACTION="list" ;;
    --help|-h|"") usage ;;
    *) echo "Unknown command: $1"; usage ;;
esac

# Security group IDs (stable unless stacks are recreated)
TRACKER_SG="sg-029f9b7d81b49a396"
SWEBENCH_SG="sg-0407fa6a5f15c4ce7"

if [[ "$ACTION" == "list" ]]; then
    echo "Tracker ALB (SG: $TRACKER_SG):"
    aws ec2 describe-security-group-rules \
        --filters "Name=group-id,Values=$TRACKER_SG" \
        --query 'SecurityGroupRules[?IsEgress==`false`].{Port:FromPort,Source:CidrIpv4||ReferencedGroupInfo.GroupId,Description:Description}' \
        --output table 2>/dev/null || echo "  (no rules)"

    echo ""
    echo "Swebench (SG: $SWEBENCH_SG):"
    aws ec2 describe-security-group-rules \
        --filters "Name=group-id,Values=$SWEBENCH_SG" \
        --query 'SecurityGroupRules[?IsEgress==`false`].{Port:FromPort,Source:CidrIpv4||ReferencedGroupInfo.GroupId,Description:Description}' \
        --output table 2>/dev/null || echo "  (no rules)"
    exit 0
fi

IP="${1:-$(curl -s https://checkip.amazonaws.com)}"
DESC="${2:-Manual whitelist}"
CIDR="${IP}/32"

echo "IP: $IP"
echo "Tracker ALB SG: $TRACKER_SG"
echo "Swebench SG: $SWEBENCH_SG"

if [[ "$ACTION" == "remove" ]]; then
    echo "Removing rules..."

    aws ec2 revoke-security-group-ingress \
        --group-id "$TRACKER_SG" \
        --protocol tcp --port 443 --cidr "$CIDR" 2>/dev/null || true

    aws ec2 revoke-security-group-ingress \
        --group-id "$SWEBENCH_SG" \
        --protocol tcp --port 8000 --cidr "$CIDR" 2>/dev/null || true

    echo "Done - removed $IP from security groups"
else
    echo "Adding rules..."

    aws ec2 authorize-security-group-ingress \
        --group-id "$TRACKER_SG" \
        --protocol tcp --port 443 --cidr "$CIDR" \
        --tag-specifications "ResourceType=security-group-rule,Tags=[{Key=Description,Value='$DESC'}]" 2>/dev/null || echo "  (tracker rule may already exist)"

    aws ec2 authorize-security-group-ingress \
        --group-id "$SWEBENCH_SG" \
        --protocol tcp --port 8000 --cidr "$CIDR" \
        --tag-specifications "ResourceType=security-group-rule,Tags=[{Key=Description,Value='$DESC'}]" 2>/dev/null || echo "  (swebench rule may already exist)"

    echo "Done - $IP can now access:"
    echo "  - Tracker: https://benchmark-tracker.vals.ai"
    echo "  - Swebench: http://<task-ip>:8000"
fi
