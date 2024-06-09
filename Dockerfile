# 1
FROM ubuntu:22.04
WORKDIR /app
RUN apt update

# 2
RUN apt -y install openjdk-21-jdk
RUN apt -y install wget
RUN wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
RUN tar xvf spark-3.5.1-bin-hadoop3.tgz
RUN rm spark-3.5.1-bin-hadoop3.tgz
RUN mv spark-3.5.1-bin-hadoop3 /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PATH=$PATH:$SPARK_HOME/sbin

# 3
RUN apt -y install sqlite3
RUN apt -y install python3
RUN apt install nano

# 4
RUN apt -y install python3-pip
RUN pip install pyspark
RUN wget https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.34.0/sqlite-jdbc-3.34.0.jar

# # 5
RUN mv sqlite-jdbc-3.34.0.jar /opt/spark/jars






