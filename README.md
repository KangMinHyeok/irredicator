# IRRedicator: Pruning IRR with RPKI-Valid BGP Insights
This is the code repo for IRRedicator. Paper is published at NDSS'2024.
<!-- 
## Bibliography
```
@inproceedings{li-2023-rov,
  author = {Weitong Li and Zhexiao Lin and Mohammad Ishtiaq Ashiq Khan and Emile Aben and Romain Fontugne and Amreesh Phokeer and Taejoong Chung},
  title = {{RoVista: Measuring and Understanding the Route Origin Validation (ROV) in RPKI}},
  booktitle = {Proceedings of the ACM Internet Measurement Conference (IMC'23)},
  address = {Montreal, Canada},
  month = {October},
  year = {2023}
}
``` -->
## Results
We make our results publicly available: https://irredicator.netsecurelab.org/

## Methodology
Border Gateway Protocol (BGP) provides a way of exchanging routing information to help routers construct their routing tables. However, due to the lack of security considerations, BGP has been suffering from vulnerabilities such as BGP hijacking attacks. 

To mitigate these issues, two data sources have been used, Internet Routing Registry (IRR) and Resource Public Key Infrastructure (RPKI), to provide reliable mappings between IP prefixes and their authorized Autonomous Systems (ASes). Each of the data sources, however, has its own limitations. IRR has been well-known for its stale Route objects with outdated AS information since network operators do not have enough incentives to keep them up to date, and RPKI has been slowly deployed due to its operational complexities. 

We measure the prevalent inconsistencies between Route objects in IRR and ROA objects in RPKI. We next characterize inconsistent and consistent Route objects, respectively, by focusing on their BGP announcement patterns. Based on this insight, we develop a technique that identifies stale Route objects by leveraging a machine learning algorithm and evaluate its performance.

## IRRedicator structure
The scripts of IRRedicator can be categorized as follows: obtaining datasets, preprocessing datasets, conducting longitudinal study, and evaluating performance.

``` shell
├── analysis
│   ├── spark_analyze_active_cdf.py # analyze cdf of active IRR records
│   ├── spark_analyze_as_coverage.py # analyze the AS coverage of IRR and RPKI 
│   ├── spark_analyze_bgp_coverage.py # analyze the BGP coverage of IRR and RPKI 
│   ├── spark_analyze_inconsistent_bgp_coverage.py # analyze the BGP coverage of inconsistent prefixes
│   ├── spark_analyze_inconsistent_objects.py # analyze the percentage/# of inconsistent prefixes
│   ├── spark_analyze_ip_coverage.py # analyze the IP coverage of IRR and RPKI
│   ├── spark_analyze_irr_age.py # analyze the age of IRR objects
│   └── spark_analyze_overlapping_objects.py # analyze the number of overlapping objects between IRR and RPKI

├── caida
│   ├── as2isp.py # get a organization of an AS 
│   └── as2isp.py # get a relationship between two ASes

├── dataset
│   ├── get_caida.py # download caida datasets
│   ├── get_irr.py # download IRR datasets
│   ├── get_nrostats.py # download NRO statistics datasets
│   ├── get_radb.py # download the RADb dataset from the archive of RADb
│   ├── get_roa.py # download ROA datasets
│   ├── get_routeviews.py # download BGP datasets
│   └── get_transfer_log.py # download transfer log datasets

├── evaluation
│   ├── evaluate_rpki_performance.py # evaluate the performance against RPKI
│   ├── evaluate_transfer_log_performance.py # evaluate the performance against transfer log
│   ├── spark_evaluate_active_records.py # evaluate the percentage of active records
│   └── spark_evaluate_bgp_coverage.py # evaluate the percentage of covered BGP announcements

├── model
│   ├── dataset.py # load BGP feature dataset
│   ├── loss.py # loss functions
│   └── model.py # IRRedicator model 

├── preprocess
│   ├── decompress_irr.py # decompressing IRR database
│   ├── preprocess_transfer_log.py # convert the format of transfer logs: json --> tsv
│   ├── spark_extract_bgp_feature.py # extract BGP features
│   ├── spark_preprocess_active.py # get active IRR records
│   ├── spark_preprocess_irr.py # parse IRR to tsv
│   └── spark_preprocess_vrp.py # Parse VRP to tsv

├── utils
│   ├── binaryPrefixTree.py # build a tree-based data structure to efficiently find covered records
│   ├── bitvector.py # create a bitvetor
│   ├── score.py # scoring the performance
    └── utils.py # utility function
```
