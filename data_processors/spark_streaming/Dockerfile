FROM apache/spark:3.5.3

USER root

WORKDIR /app

COPY spark_processor.py /app/

CMD /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    --master local[*] \
    spark_processor.py