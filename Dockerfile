FROM apache/spark:3.5.3

USER root

RUN apt-get update && apt-get install -y python3-pip wget openjdk-11-jdk && \
    pip3 install --no-cache-dir psutil pandas matplotlib seaborn && \
    rm -rf /var/lib/apt/lists/*

ENV HADOOP_VERSION=3.2.1
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV PATH=$PATH:$HADOOP_HOME/bin

RUN mkdir -p /benchmark/data /benchmark/src /benchmark/logs /benchmark/plots

RUN chmod -R 777 /benchmark

USER spark
WORKDIR /opt/spark