Write-Host 'Creating Python virtual environment "scripts/.venv"'
python -m venv .\scripts\.venv

Write-Host 'Installing dependencies from "requirements.txt" into virtual environment'
.\scripts\.venv\Scripts\python -m pip install -r .\requirements.txt

.\scripts\.venv\Scripts\python setup.py -s $env:AZURE_SUBSCRIPTION_ID -r $env:AZURE_DATA_INGEST_FUNC_RG -f $env:AZURE_DATA_INGEST_FUNC_NAME -a $env:AZURE_SEARCH_PRINCIPAL_ID -m $env:AZURE_SEARCH_USE_MIS
