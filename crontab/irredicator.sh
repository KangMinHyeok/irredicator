#!/bin/bash
export HADOOP_HOME=/usr/local/hadoop
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=/usr/local/hadoop/bin:/usr/local/hadoop/sbin:/usr/local/spark/bin:/home/mhkang/anaconda3/bin:$PATH
export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
export SPARK_HOME=/usr/local/hadoop/spark
export LD_LIBRARY_PATH=/usr/local/hadoop/lib/native:$LD_LIBRARY_PATH
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$PYTHONPATH


python3.8 /home/mhkang/rpki-irr/irredicator/evaluation/apply_irredicator.py  >> /home/mhkang/rpki-irr/logs/apply_irredicator.log 2>&1
python3.8 /home/mhkang/rpki-irr/irredicator/dataset/put_dataset.py --local_dir /home/mhkang/rpki-irr/outputs/irredicator/records/ --hdfs_dir /user/mhkang/irredicator/records/ --suffix tsv --logfile put_irredicator_records.log
/home/mhkang/.local/bin/spark-submit /home/mhkang/rpki-irr/irredicator/evaluation/spark_irredicator_records_to_overview.py >> /home/mhkang/rpki-irr/logs/spark_irredicator_records_to_overview.log 2>&1
python3.8 /home/mhkang/rpki-irr/irredicator/evaluation/copy_overview.py  >> /home/mhkang/rpki-irr/logs/copy_overview.log 2>&1