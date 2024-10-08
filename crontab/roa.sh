#!/bin/bash
export HADOOP_HOME=/usr/local/hadoop
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=/usr/local/hadoop/bin:/usr/local/hadoop/sbin:/usr/local/spark/bin:/home/mhkang/anaconda3/bin:$PATH
export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
export SPARK_HOME=/usr/local/hadoop/spark
export LD_LIBRARY_PATH=/usr/local/hadoop/lib/native:$LD_LIBRARY_PATH
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$PYTHONPATH


python3.8 /home/mhkang/rpki-irr/irredicator/dataset/get_roa.py >> /home/mhkang/rpki-irr/logs/get_roa.log 2>&1
/usr/local/hadoop/bin/hdfs dfs -put /net/data/vrps/ziggy-vrps/*.csv /user/mhkang/vrps/ziggy-vrps/ >> /home/mhkang/rpki-irr/logs/put-ziggy-vrp.log 2>&1
/home/mhkang/.local/bin/spark-submit /home/mhkang/rpki-irr/irredicator/preprocess/spark_preprocess_vrp.py >> /home/mhkang/rpki-irr/logs/spark_preprocess_vrp.log 2>&1
/usr/local/hadoop/bin/hdfs dfs -put /net/data/vrps/daily-tsv/*.tsv /user/mhkang/vrps/daily-tsv/ >> /home/mhkang/rpki-irr/logs/put-vrp-daily-tsv.log 2>&1
