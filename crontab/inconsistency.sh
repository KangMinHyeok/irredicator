#!/bin/bash
export HADOOP_HOME=/usr/local/hadoop
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=/usr/local/hadoop/bin:/usr/local/hadoop/sbin:/usr/local/spark/bin:/home/mhkang/anaconda3/bin:$PATH
export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
export SPARK_HOME=/usr/local/hadoop/spark
export LD_LIBRARY_PATH=/usr/local/hadoop/lib/native:$LD_LIBRARY_PATH
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$PYTHONPATH

/home/mhkang/.local/bin/spark-submit /home/mhkang/rpki-irr/irredicator/analysis/spark_analyze_inconsistent_prefixes.py >> /home/mhkang/rpki-irr/logs/spark_analyze_inconsistent_prefixes.log 2>&1
# /home/mhkang/.local/bin/spark-submit /home/mhkang/rpki-irr/irredicator/analysis/spark_analyze_inconsistent_bgp_coverage.py >> /home/mhkang/rpki-irr/logs/spark_analyze_inconsistent_bgp_coverage.log 2>&1
