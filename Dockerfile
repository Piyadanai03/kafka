# 1. เริ่มต้นจาก Image เดิม
FROM debezium/connect:2.6

USER root

# 2. สร้างโฟลเดอร์ "เดียว" สำหรับ Plugin ทั้งหมด
RUN mkdir -p /kafka/connect/my-plugins

# 3. ติดตั้ง JARs ทั้งหมด (ทั้ง Avro และ JDBC) ลงในโฟลเดอร์ "เดียว" นั้น
RUN cd /tmp && \
    \
    # --- Avro Converter + All Dependencies ---
    curl -sLO https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/7.6.0/kafka-connect-avro-converter-7.6.0.jar && \
    curl -sLO https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-data/7.6.0/kafka-connect-avro-data-7.6.0.jar && \
    curl -sLO https://packages.confluent.io/maven/io/confluent/kafka-schema-converter/7.6.0/kafka-schema-converter-7.6.0.jar && \
    curl -sLO https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/7.6.0/kafka-avro-serializer-7.6.0.jar && \
    curl -sLO https://packages.confluent.io/maven/io/confluent/kafka-schema-serializer/7.6.0/kafka-schema-serializer-7.6.0.jar && \
    curl -sLO https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/7.6.0/kafka-schema-registry-client-7.6.0.jar && \
    curl -sLO https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.3/avro-1.11.3.jar && \
    curl -sLO https://repo1.maven.org/maven2/com/google/guava/guava/32.1.3-jre/guava-32.1.3-jre.jar && \
    curl -sLO https://repo1.maven.org/maven2/com/google/guava/failureaccess/1.0.1/failureaccess-1.0.1.jar && \
    \
    # --- JDBC Sink Connector + Driver ---
    curl -sLO https://repo1.maven.org/maven2/io/debezium/debezium-connector-jdbc/2.6.2.Final/debezium-connector-jdbc-2.6.2.Final.jar && \
    curl -sLO https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.3/postgresql-42.5.3.jar && \
    \
    # --- ย้ายไฟล์ .jar ทั้งหมดไปที่ my-plugins ---
    mv *.jar /kafka/connect/my-plugins/

# 4. สลับกลับเป็น kafka user
USER kafka