FROM bde2020/spark-java-template:2.2.0-hadoop2.7

ENV SPARK_APPLICATION_JAR_NAME=oome-reproducer-1.0-SNAPSHOT-with-dependencies
ENV SPARK_APPLICATION_MAIN_CLASS=org.example.oome.App
ENV ENABLE_INIT_DAEMON=false