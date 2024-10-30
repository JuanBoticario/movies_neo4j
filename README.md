# movies_neo4j
Simple example Python program using Neo4J driver.

## Requirements

- Python 3.7+
- Docker 

## Setup Instructions

Clone the repository: 
```bash
git clone https://github.com/JuanBoticario/movies_neo4j
cd movies-neo4j
```

Download and extract the datasets:
```bash
mkdir data && cd data
wget https://datasets.imdbws.com/name.basics.tvs.gz
wget https://datasets.imdbws.com/title.basics.tvs.gz
wget https://datasets.imdbws.com/title.principals.tvs.gz
7z -x *
```

Install the necessary python libraries:
```bash
pip -r requirements.txt
```

Get the Neo4J container up:
```bash
docker compose up -d 
```

Now you can start using the program. To dump the datasets into the database (only names and titles):
```python
python3 main.py dump all
```

