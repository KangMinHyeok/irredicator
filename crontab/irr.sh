#!/bin/bash
export HADOOP_HOME=/usr/local/hadoop
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=/usr/local/hadoop/bin:/usr/local/hadoop/sbin:/usr/local/spark/bin:/home/mhkang/anaconda3/bin:$PATH
export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
export SPARK_HOME=/usr/local/hadoop/spark
export LD_LIBRARY_PATH=/usr/local/hadoop/lib/native:$LD_LIBRARY_PATH
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$PYTHONPATH


python3.8 /home/mhkang/rpki-irr/irredicator/dataset/get_irr.py >> /home/mhkang/rpki-irr/logs/get_irr.log 2>&1
/usr/local/hadoop/bin/hdfs dfs -put /net/data/irrs/compressed/*.db.gz /user/mhkang/irrs/rawdata/ >> /home/mhkang/rpki-irr/logs/put-irrs.log 2>&1
/usr/local/hadoop/bin/hdfs dfs -put /net/data/irrs/compressed/*.db.route.gz /user/mhkang/irrs/rawdata/ >> /home/mhkang/rpki-irr/logs/put-irrs-rawdata.log 2>&1
/home/mhkang/.local/bin/spark-submit /home/mhkang/rpki-irr/irredicator/preprocess/spark_preprocess_irr.py >> /home/mhkang/rpki-irr/logs/spark_preprocess_irr.log
/usr/local/hadoop/bin/hdfs dfs -put /net/data/irrs/daily-tsv/*.tsv /user/mhkang/irrs/daily-tsv/ >> /home/mhkang/rpki-irr/logs/put-irrs-daily-tsv.log 2>&1
