#!/bin/sh

BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo ""
echo "------------- NOTE -------------"
echo "azd provision and azd up are ${YELLOW}not allowed${NC} for this project."
echo "Infrastructure is defined in https://github.com/Azure/GPT-RAG."
echo "After deploying infrastructure, run ${BLUE}azd env refresh${NC} with the same environment name, subscription and location."
echo "Then run ${BLUE}azd deploy${NC}"

exit 1
