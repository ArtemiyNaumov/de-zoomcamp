FROM prefecthq/prefect:2.7.7-python3.11

COPY docker_requirements.txt .

RUN pip install -r docker_requirements.txt --trusted-host pypi.python.org --no-cache-dir
RUN mkdir /opt/prefect/flows
RUN mkdir /opt/prefect/data

COPY parametrized_flow.py /opt/prefect/flows/parametrized_flow.py
COPY data /opt/prefect/data