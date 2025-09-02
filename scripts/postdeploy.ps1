Write-Host 'Creating Python virtual environment "scripts/.venv"'
python -m venv .\scripts\.venv

Write-Host 'Installing dependencies from "requirements.txt" into virtual environment'
.\scripts\.venv\Scripts\python -m pip install -r .\requirements.txt

Write-Host 'Skipping setup.py (removed). Search resources are provisioned elsewhere.'
