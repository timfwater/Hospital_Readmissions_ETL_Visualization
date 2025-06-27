FROM openjdk:8-jdk-slim

# Install Python and pip, and symlink python → python3
RUN apt-get update && \
    apt-get install -y python3 python3-pip curl && \
    pip3 install --upgrade pip && \
    ln -s /usr/bin/python3 /usr/bin/python

# Set working directory
WORKDIR /app

# Copy all project files
COPY . .

# Install Python dependencies
RUN pip3 install -r requirements.txt

# Install Spark
ENV SPARK_VERSION=3.3.2
ENV HADOOP_VERSION=3

RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar xz -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

# Run the pipeline
CMD ["python", "run_pipeline.py"]
