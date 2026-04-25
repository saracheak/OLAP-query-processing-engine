# OLAP-query-processing-engine
Ad-hoc OLAP queries expressed in standard SQL often lead to complex relational algebraic expressions. We provide a framework to allow succinct expression of ad-hoc OLAP queries by extending the group-by statement and adding the new clause, such that, which provides a simple and scalable algorithm to process OLAP queries.

# How to Run the Program
## Requirements
- Python 3
- PostgreSQL installed and running

## Setup
1. Setup Python Environment:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

Install PostgreSQL (Mac) if not already installed:
```bash
brew install postgresql
brew services start postgresql

Note: You may need to change your postgres password using ALTER USER postgres WITH PASSWORD 'password';

2. Setup PostgreSQL Database:
```bash
createdb sales
psql -U postgres -d sales -f setup_sales.sql

## (Optional) Set PostgreSQL Password
If you are prompted for a password and do not know it, run:

```bash
psql -U postgres
ALTER USER postgres WITH PASSWORD 'password';
\q

```md
Alternatively, you can use your own PostgreSQL username and update the connection settings in the code.
