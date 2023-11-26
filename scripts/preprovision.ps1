Write-Host ""
Write-Host "------------- NOTE -------------"
Write-Host "azd provision and azd up are not allowed for this project."
Write-Host "Infrastructure is defined in https://github.com/Azure/GPT-RAG."
Write-Host "After deploying infrastructure, run azd env refresh with the same environment name, subscription and location."
Write-Host "Then run azd deploy"

exit 1
