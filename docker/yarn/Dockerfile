FROM eclipse-temurin:11-jre

ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root
ENV PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# 使用阿里云 Ubuntu 镜像加速 apt
RUN sed -i 's@archive.ubuntu.com@mirrors.aliyun.com@g' /etc/apt/sources.list \
  && apt-get update && apt-get install -y curl \
  && rm -rf /var/lib/apt/lists/*

# 使用腾讯云 Hadoop 镜像加速
RUN curl -fsSL \
    "https://mirrors.cloud.tencent.com/apache/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
  | tar -xz -C /opt/ \
  && mv "/opt/hadoop-${HADOOP_VERSION}" "${HADOOP_HOME}"

COPY config/*.xml ${HADOOP_HOME}/etc/hadoop/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 9870 9000 8088 8042 8032 8030 8031 8033

CMD ["/entrypoint.sh"]
