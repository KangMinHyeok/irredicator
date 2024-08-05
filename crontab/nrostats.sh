#!/bin/bash
python3.8 /home/mhkang/rpki-irr/irredicator/dataset/get_nrostats.py >> /home/mhkang/rpki-irr/logs/get_nrostats.log 2>&1
/usr/local/hadoop/bin/hdfs dfs -put /home/mhkang/nrostats/asn/*.csv /user/mhkang/nrostats/asn/ >> /home/mhkang/rpki-irr/logs/put-nrostats-asn.log 2>&1
/usr/local/hadoop/bin/hdfs dfs -put /home/mhkang/nrostats/ipv4-w-date/*.csv /user/mhkang/nrostats/ipv4-w-date/ >> /home/mhkang/rpki-irr/logs/put-nrostats-ipv4.log 2>&1
/usr/local/hadoop/bin/hdfs dfs -put /home/mhkang/nrostats/ipv6-w-date/*.csv /user/mhkang/nrostats/ipv6-w-date/ >> /home/mhkang/rpki-irr/logs/put-nrostats-ipv6.log 2>&1