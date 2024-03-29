FROM python:3.9-slim

# Install compilers
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
    --no-install-recommends build-essential && \
    apt-get install -y postgresql-client

# create user
RUN useradd --create-home --shell /bin/bash user

COPY requirements.txt .
RUN pip install --no-cache-dir -r ./requirements.txt \
    && rm -f requirements.txt

# create working directory
RUN mkdir home/user/pgcom
ADD . home/user/pgcom

# install deps
RUN cd /home/user/pgcom && \
    pip install --no-cache-dir --editable .[test]

# grant user permissions
RUN chown -R user:user /home/user
USER user

WORKDIR /home/user/pgcom