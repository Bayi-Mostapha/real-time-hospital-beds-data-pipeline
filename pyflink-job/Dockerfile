FROM flink:1.18-scala_2.12-java11

RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-dev netcat-openbsd wget && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

RUN cp /opt/flink/plugins/metrics-prometheus/flink-metrics-prometheus-1.18.1.jar /opt/flink/lib/

RUN wget -q -O /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

COPY consumer.py /opt/flink/consumer.py