# https://github.com/buildkite/agent/blob/master/packaging/docker/ubuntu-linux/Dockerfile
FROM python:3.6

# install common utilities

ARG pypi_user
ARG pypi_pass

ENV LANG "en_US.UTF-8"
ENV LANGUAGE "en_US.UTF-8"
ENV LC_ALL "en_US.UTF-8"
ENV PYPI_USERNAME ${pypi_user}
ENV PYPI_PASSWORD ${pypi_pass}

COPY . /pycarol
WORKDIR /pycarol

RUN pip install -r requirements.txt && \
    pip install nose coverage nose-cover3 twine && \
    echo -e "[distutils]" > ~/.pypirc && \
    echo -e "index-servers=" >> ~/.pypirc && \
    echo -e "    totvslabs" >> ~/.pypirc && \
    echo -e "[totvslabs]" >> ~/.pypirc && \
    echo -e "repository: http://nexus3.carol.ai:8080/repository/totvslabspypi/pypi" >> ~/.pypirc && \
    echo -e "username: ${PYPI_USERNAME}" >> ~/.pypirc && \
    echo -e "password: ${PYPI_PASSWORD}" >> ~/.pypirc && \
    pip config set global.index http://nexus3.carol.ai:8080/repository/totvslabspypi/pypi && \
    pip config set global.index-url http://nexus3.carol.ai:8080/repository/totvslabspypi/simple

RUN make ci