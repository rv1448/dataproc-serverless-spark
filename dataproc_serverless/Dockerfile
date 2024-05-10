FROM debian:11-slim

# Suppress interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install necessary tools
RUN apt-get update && apt-get install -y wget procps tini curl \
    && rm -rf /var/lib/apt/lists/*
 
ENV CONDA_HOME=/opt/miniconda3
ENV PYSPARK_PYTHON=${CONDA_HOME}/bin/python
ENV PATH=${CONDA_HOME}/bin:${PATH}


RUN wget https://repo.anaconda.com/miniconda/Miniconda3-py39_24.1.2-0-Linux-x86_64.sh -O Miniconda3-installer.sh \
    && sh Miniconda3-installer.sh -b -p ${CONDA_HOME} \
    && rm Miniconda3-installer.sh

# Configure Conda settings
RUN conda config --system --set always_yes True \
    && conda config --system --set auto_update_conda False \
    && conda config --system --prepend channels conda-forge \
    && conda config --system --set channel_priority strict

RUN ${CONDA_HOME}/bin/python -m pip install -v rapidgzip
#  bigframes

# Downloading gcloud package
RUN curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz

# Installing the package
RUN mkdir -p /usr/local/gcloud 
RUN tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz
RUN cat /usr/local/gcloud/google-cloud-sdk/install.sh 
RUN /usr/local/gcloud/google-cloud-sdk/install.sh 

# Adding the package path to local
ENV PATH=$PATH:/usr/local/gcloud/google-cloud-sdk/bin

RUN apt-get clean \
    && conda clean -afy

RUN groupadd -g 1099 spark
RUN useradd -u 1099 -g 1099 -d /home/spark -m spark
USER spark