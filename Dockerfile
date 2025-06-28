FROM openjdk:8-jdk-slim

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip curl && \
    apt-get clean

# Set working directory
WORKDIR /app

# Copy all project files
COPY . .

# Install Python dependencies
RUN pip3 install -r requirements.txt

# Install Spark (but not used for this test)
ENV SPARK_VERSION=3.3.2
ENV HADOOP_VERSION=3

RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar xz -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="/opt/spark/bin:$PATH"

# TEMPORARY TEST CMD: run the pipeline using python3 directly
CMD ["python3", "run_pipeline.py"]
