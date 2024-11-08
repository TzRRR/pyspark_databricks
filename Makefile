install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

elt:
	python main.py

test:
	python -m pytest -vv --cov=main --cov=mylib test_*.py

format:	
	black *.py 

lint:
	ruff check *.py mylib/*.py
