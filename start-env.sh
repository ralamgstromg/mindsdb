python3.11 -m venv mindsdb-venv
source mindsdb-venv/bin/activate
pip install --upgrade pip setuptools wheel
pip install -e .
pip install -r requirements/requirements.txt
#pip install -r requirements/requirements-dev.txt
python -m mindsdb --api http,mysql,mongodb,postgres,mcp --config mindsdb_config.json
# cd mindsdb
# pip install .[handler_name]

export MINDSDB_USERNAME=root
export MINDSDB_PASSWORD=


unset MINDSDB_USERNAME
unset MINDSDB_PASSWORD



python -m mindsdb --api mysql,mongodb,postgres,mcp --config mindsdb_config.json


python -m mindsdb --api mysql --config mindsdb_config.json

python -m mindsdb --api mysql,http --config mindsdb_config.json


git config --global user.email "you@example.com"
git config --global user.name "Your Name"