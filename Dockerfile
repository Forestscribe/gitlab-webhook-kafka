from pypy:2-slim
add Pipfile* /app/
workdir /app
run pip install -U pip pipenv && pipenv install
add application.py /app/
