echo 'Creating Python virtual environment "scripts/.venv"'
python3.11 -m venv ./scripts/.venv

echo 'Upgrading pip, setuptools, and wheel in virtual environment'
./scripts/.venv/bin/python3.11 -m pip install --upgrade pip setuptools wheel

echo 'Installing dependencies from "requirements.txt" into virtual environment'
./scripts/.venv/bin/python3.11 -m pip install -r ./requirements.txt

echo 'Running "setup.py" to create/update AI search components'
./scripts/.venv/bin/python3.11 setup.py -s $AZURE_SUBSCRIPTION_ID -r $AZURE_DATA_INGEST_FUNC_RG -f $AZURE_DATA_INGEST_FUNC_NAME -a $AZURE_SEARCH_PRINCIPAL_ID -m $AZURE_SEARCH_USE_MIS

echo 'Done postdeploy.sh'
