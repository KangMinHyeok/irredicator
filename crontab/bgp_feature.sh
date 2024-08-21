#!/bin/bash
export HADOOP_HOME=/usr/local/hadoop
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=/usr/local/hadoop/bin:/usr/local/hadoop/sbin:/usr/local/spark/bin:/home/mhkang/anaconda3/bin:$PATH
export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
export SPARK_HOME=/usr/local/hadoop/spark
export LD_LIBRARY_PATH=/usr/local/hadoop/lib/native:$LD_LIBRARY_PATH
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$PYTHONPATH


/home/mhkang/.local/bin/spark-submit /home/mhkang/rpki-irr/irredicator/preprocess/spark_bgp_bitvector.py >> /home/mhkang/rpki-irr/logs/spark_bgp_bitvector.log 2>&1
# /usr/local/hadoop/bin/hdfs dfs -put /net/data/routeviews/bitvector/*.tsv /user/mhkang/routeviews/bitvector/ >> /home/mhkang/rpki-irr/logs/put-bitvectors.log 2>&1
python3.8 /home/mhkang/rpki-irr/irredicator/dataset/put_dataset.py --local_dir /net/data/routeviews/bitvector/ --hdfs_dir /user/mhkang/routeviews/bitvector/ --suffix tsv --logfile put_bitvectors.log
/home/mhkang/.local/bin/spark-submit /home/mhkang/rpki-irr/irredicator/preprocess/spark_extract_bgp_feature.py >> /home/mhkang/rpki-irr/logs/spark_extract_bgp_feature.log 2>&1
# /usr/local/hadoop/bin/hdfs dfs -put /net/data/routeviews/feature/*.tsv /user/mhkang/routeviews/feature/ >> /home/mhkang/rpki-irr/logs/put-features.log 2>&1
python3.8 /home/mhkang/rpki-irr/irredicator/dataset/put_dataset.py --local_dir /net/data/routeviews/feature/ --hdfs_dir /user/mhkang/routeviews/feature/ --suffix tsv --logfile put_features.log