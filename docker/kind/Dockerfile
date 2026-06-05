ARG BASE_IMAGE
FROM ${BASE_IMAGE}
# 将 SQL Gateway JAR 从 opt 移到 lib，使 Flink 运行时自动加载
RUN mv /opt/flink/opt/flink-sql-gateway-*.jar /opt/flink/lib/ 2>/dev/null || true
COPY app.jar                  /opt/flink/usrlib/app.jar
COPY example-udf-reverse.jar  /opt/flink/usrlib/
COPY example-udf-substring.jar /opt/flink/usrlib/
