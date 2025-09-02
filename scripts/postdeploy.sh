echo 'Creating Python virtual environment "scripts/.venv"'
python3.11 -m venv ./scripts/.venv

echo 'Upgrading pip, setuptools, and wheel in virtual environment'
./scripts/.venv/bin/python3.11 -m pip install --upgrade pip setuptools wheel

echo 'Installing dependencies from "requirements.txt" into virtual environment'
./scripts/.venv/bin/python3.11 -m pip install -r ./requirements.txt

echo 'Done postdeploy.sh'
