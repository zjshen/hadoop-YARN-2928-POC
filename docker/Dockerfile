#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Use ubuntu 14.04
FROM ubuntu:14.04
MAINTAINER Zhijie Shen <zjshen14@gmail.com>

# Installing required packages 
RUN apt-get -y update
RUN apt-get -y install openjdk-7-jdk

# Setup users
RUN addgroup hadoop \
&&  adduser --ingroup hadoop --disabled-password --gecos "" hdfs \
&&  adduser --ingroup hadoop --disabled-password --gecos "" yarn \
&&  adduser --ingroup hadoop --disabled-password --gecos "" mapred

# Setup environment variables
ENV JAVA_HOME /usr/lib/jvm/java-7-openjdk-amd64
ENV HADOOP_VERSION 3.0.0-SNAPSHOT
ENV HADOOP_HOME /opt/hadoop-${HADOOP_VERSION}
ENV HADOOP_MAPRED_HOME /opt/hadoop-${HADOOP_VERSION}
ENV HADOOP_COMMON_HOME /opt/hadoop-${HADOOP_VERSION}
ENV HADOOP_HDFS_HOME /opt/hadoop-${HADOOP_VERSION}
ENV HADOOP_YARN_HOME /opt/hadoop-${HADOOP_VERSION}
ENV HADOOP_CONF_DIR /etc/hadoop/conf
ENV YARN_CONF_DIR ${HADOOP_CONF_DIR}
ENV HADOOP_LOG_DIR /var/log/hadoop
ENV YARN_LOG_DIR /var/log/hadoop-yarn
ENV HBASE_LOG_DIR /var/log/hbase
ENV PATH $PATH:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:/opt/hbase-1.0.1.1/bin

RUN echo "export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64" >> /etc/hadoop-env.sh \
&&  echo "export HADOOP_VERSION=3.0.0-SNAPSHOT" >> /etc/bash.bashrc \
&&  echo "export HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}" >> /etc/hadoop-env.sh \
&&  echo "export HADOOP_MAPRED_HOME=/opt/hadoop-${HADOOP_VERSION}" >> /etc/hadoop-env.sh \
&&  echo "export HADOOP_COMMON_HOME=/opt/hadoop-${HADOOP_VERSION}" >> /etc/hadoop-env.sh \
&&  echo "export HADOOP_HDFS_HOME=/opt/hadoop-${HADOOP_VERSION}" >> /etc/hadoop-env.sh \
&&  echo "export HADOOP_YARN_HOME=/opt/hadoop-${HADOOP_VERSION}" >> /etc/hadoop-env.sh \
&&  echo "export HADOOP_CONF_DIR=/etc/hadoop/conf" >> /etc/hadoop-env.sh \
&&  echo "export YARN_CONF_DIR=${HADOOP_CONF_DIR}" >> /etc/hadoop-env.sh \
&&  echo "export HADOOP_LOG_DIR=/var/log/hadoop" >> /etc/hadoop-env.sh \
&&  echo "export YARN_LOG_DIR=/var/log/hadoop-yarn" >> /etc/hadoop-env.sh \
&&  echo "export HBASE_LOG_DIR=/var/log/hbase" >> /etc/hadoop-env.sh \
&&  echo "export PATH=$PATH:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:/opt/hbase-1.0.1.1/bin" >> /etc/hadoop-env.sh

# Install and config Hadoop components
ADD ./hadoop-3.0.0-SNAPSHOT.tar.gz /opt
ADD ./hbase-1.0.1.1-bin.tar.gz /opt
RUN mkdir /etc/hadoop \
&&  mv /opt/hadoop-${HADOOP_VERSION}/etc/hadoop ${HADOOP_CONF_DIR} \
&&  rmdir /opt/hadoop-${HADOOP_VERSION}/etc
ADD ./conf ${HADOOP_CONF_DIR}
RUN cp ${HADOOP_CONF_DIR}/hbase-site.xml /opt/hbase-1.0.1.1/conf/hbase-site.xml

RUN mkdir ${HADOOP_LOG_DIR} \
&&  mkdir ${YARN_LOG_DIR} \
&&  mkdir /var/log/hbase \
&&  mkdir -p /var/db/hbase/data \
&&  chown hdfs:hadoop ${HADOOP_LOG_DIR} \
&&  chown yarn:hadoop ${YARN_LOG_DIR} \
&&  chown yarn:hadoop /var/log/hbase \
&&  chown yarn:hadoop /opt/hbase-1.0.1.1

RUN sudo su - -c "source /etc/hadoop-env.sh && hdfs namenode -format" hdfs
RUN echo "#!/bin/bash" >> /bin/start-hadoop.sh \
&&  echo 'sudo su - -c "source /etc/hadoop-env.sh && hadoop-daemon.sh start namenode" hdfs' >> /bin/start-hadoop.sh \
&&  echo 'sudo su - -c "source /etc/hadoop-env.sh && hadoop-daemon.sh start datanode" hdfs' >> /bin/start-hadoop.sh \
&&  echo 'sudo su - -c "source /etc/hadoop-env.sh && hdfs dfs -mkdir /hadoop-yarn" hdfs' >> /bin/start-hadoop.sh \
&&  echo 'sudo su - -c "source /etc/hadoop-env.sh && hdfs dfs -chown yarn:hadoop /hadoop-yarn" hdfs' >> /bin/start-hadoop.sh \
&&  echo 'sudo su - -c "source /etc/hadoop-env.sh && hdfs dfs -mkdir -p /user/yarn" hdfs' >> /bin/start-hadoop.sh \
&&  echo 'sudo su - -c "source /etc/hadoop-env.sh && hdfs dfs -chown yarn /user/yarn" hdfs' >> /bin/start-hadoop.sh \
&&  echo 'sudo su - -c "source /etc/hadoop-env.sh && /opt/hbase-1.0.1.1/bin/start-hbase.sh" yarn' >> /bin/start-hadoop.sh \
&&  echo 'sudo su - -c "source /etc/hadoop-env.sh && hadoop org.apache.hadoop.yarn.server.timelineservice.storage.TimelineSchemaCreator" yarn' >> /bin/start-hadoop.sh \
&&  echo 'sudo su - -c "source /etc/hadoop-env.sh && yarn-daemon.sh start resourcemanager" yarn' >> /bin/start-hadoop.sh \
&&  echo 'sudo su - -c "source /etc/hadoop-env.sh && yarn-daemon.sh start nodemanager" yarn' >> /bin/start-hadoop.sh \
&&  echo 'sudo su - -c "source /etc/hadoop-env.sh && yarn-daemon.sh start timelinereader" yarn' >> /bin/start-hadoop.sh \
&&  echo 'sudo su - -c "source /etc/hadoop-env.sh && mr-jobhistory-daemon.sh start historyserver" yarn' >> /bin/start-hadoop.sh \
&&  chmod a+x /bin/start-hadoop.sh

EXPOSE 8088 8188

CMD ["/bin/bash"]

