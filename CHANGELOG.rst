3.7.1 (August 17, 2020)
----------------------------
* Fix missing rf_mismatch arg in process_assignment call

3.7.0 (June 25, 2020)
----------------------------
* Allow dynamic_import to find the child-most class

3.6.0 (April 2, 2020)
----------------------------
* Allow kafka-rolling-restart script to be executed in clusters with SSL or SASL enabled (thanks dinegri!)

3.5.0 (March 10, 2020)
----------------------------
* Add --use-admin-client option to kafka-consumer-manager commands increased faster performance.

3.4.0 (March 4, 2020)
----------------------------
* Allow set_replication_factor command to work with topics that have mismatched replication
  factors in their partitions
* Add new preferred replica election command to generate json files for kafka-preferred-replica-election

3.3.0 (February 26, 2020)
----------------------------
* Allow for removing brokers in kafka-cluster-manager replace command
* Support consumer offset message value schema v3 (thanks mborst!)

3.2.0 (January 3, 2020)
----------------------------
* Add --active-controller-for-last flag to kafka-rolling-restart

3.1.0 (November 5, 2019)
----------------------------
* Use v1.4.7 of kafka-python to take advantage of performance upgrades
* Fix Travis build by unpinning Docker version

3.0.3 (September 18, 2019)
----------------------------
* Fix git tag

3.0.2 (September 18, 2019)
----------------------------
* Relax pytz pinned version in setup.py

3.0.1 (July 19, 2019)
----------------------------
* Fix configuration writing in kafka-manual-throttle

3.0.0 (July 16, 2019)
----------------------------
* Remove support for python3.4

2.3.1 (July 08, 2019)
----------------------------
* Fix iteration bug in _extract_actions_unique_topics with --max-movement-size

2.3.0 (May 08, 2019)
----------------------------
* Implement --head option for kafka-check command

2.2.3 (May 03, 2019)
----------------------------
* Fix bug with --max-movement-size default value causing stuck decommissions

2.2.2 (April 30, 2019)
----------------------------
* Relax requirements for PyYAML and pytz

2.2.1 (April 26, 2019)
----------------------------
* Use openjdk instead of oracle-java8 in acceptance tests
* Migrate to ubuntu 16.04 in acceptance tests
* Update team name and email

2.2.0 (April 8, 2019)
----------------------------
* Add new --force-progress flag in kafka-cluster-manager decommission
* Fix bug with moving 0-size partitions in decommission
* Fix bug with genetic balancer using size in decommission when --max-movement-size not specified

2.1.0 (April 3, 2019)
----------------------------
* Add new --max-movement-size and --auto-max-movement-size in kafka-cluster-manager decommission
* Upgrade dependency requirements to newer versions
* Fix bug with KafkaGroupReader acceptance test

2.0.0 (January 29, 2019)
----------------------------
* Deprecate zookeeper offset storage in kafka-consumer-manager

1.8.0 (January 3, 2019)
----------------------------
* Implement kafka-check replication_factor command

1.7.5 (December 3, 2018)
----------------------------
* Fix imports, pytest version and build failure due to flake8

1.7.4 (Novemeber 26, 2018)
----------------------------
* Add change to display generated reassignment plan even on failure of validation

1.7.3 (October 11, 2018)
----------------------------
* Fix bug in offset_get command that showed topics as unsubscribed if any
  partition had an offset of 0 committed

1.7.2 (October 3, 2018)
----------------------------
* Upgrade paramiko in kafka-utils (paramiko < 2.5.0)

1.7.1 (September 7, 2018)
----------------------------
* Fix bug in unsubscribe_topics command where all subscribed topics were being
  displayed as subject to change regardless of specified --topics

1.7.0 (September 6, 2018)
----------------------------
* Add new commands offset_set_timestamp and offsets_for_timestamp

1.6.8 (August 15, 2018)
----------------------------
* Add verbosity option for kafka-consumer-manager

1.6.7 (August 15, 2018)
----------------------------
* Bump setuptools version.

1.6.6 (August 14, 2018)
----------------------------
* Remove cryptography dependency

1.6.5 (July 16th, 2018)
----------------------------
* Add tox extension
* Upgrade kafka-python version to 1.4.2

1.6.4 (June 13th, 2018)
----------------------------
* Improve performance of broker decommission process

1.6.3 (June 7th, 2018)
----------------------------
* kafka-check offline_partitions does not throw an exception for empty clusters anymore
* Add --broker-ids option to kafka-rolling-restart

1.6.2 (June 5th, 2018)
----------------------------
* Fix offset_get and delete_group when no offsets folder under zk consumers node

1.6.1 (May 31st, 2018)
----------------------------
* Add NoNodeError to get_brokers & get_topics
* Catch Exceptions for commands under empty cluster exception

1.6.0 (May 29th, 2018)
----------------------------
* Set exactly topic matching as default for get_topic_watermark command
* Add -r/--regex option for get_topic_watermark command for regex search

1.5.1 (May 11th, 2018)
----------------------------
* Improve handling of missing topic in getting topic-specific configuration

1.5.0 (April 4th, 2018)
----------------------------
* Add --topics option to kafka_consumer_manager unsubscribe_topics

1.4.2 (March 20th, 2018)
----------------------------
* List unavailable-brokers in case of unavailable-replicas

1.4.1 (February 13th, 2018)
----------------------------
* Update CHANGELOG.rst for version 1.4.0

1.4.0 (February 9th, 2018)
----------------------------
* Add fetching creation time of topic and partition from zookeeper
* Fix build for kafka 0.10

1.3.3 (September 26th, 2017)
----------------------------
* Refresh ssh connection after post_stop task (simplesteph)

1.3.2 (September 13th, 2017)
----------------------------
* Add ssh config support to kafka-rolling-restarat (stephane)
* Add custom start and stop command to kafka-rolling-restart (stephane)
* Fix documentation for offset_get command

1.3.1 (August 25th, 2017)
-----------------------
* Add unhandled exception logging to kafka-cluster-manager
* Fix kafka-cluster-manager-argument

1.3.0 (July 31st, 2017)
-----------------------
* Add partition count and leader count to genetic rebalancer criterias

1.2.0 (June 19th, 2017)
-----------------------
* Add python3 support (kennydo)
* Remove fabric dependency and use paramiko (jparkie)

1.1.1 (June 5th, 2017)
----------------------
* Fix kafka topic config setter

1.1.0 (May 15th, 2017)
----------------------
* Add revoke-leadership feature in kafka-cluster-manager

1.0.3 (May 11th, 2017)
----------------------
* Bump kafka-python to 1.3.3

1.0.2 (May 11th, 2017)
----------------------
* Fix genetic balancer generation limit

1.0.1 (April 12th, 2017)
-----------------------
* Bump version to fix v1.0.0 tagging issue

1.0.0 (April 7th, 2017)
-----------------------
* Bump version to change command from under_replicated to replica_unavailability

0.6.12 (April 6th, 2017)
------------------------
* Refactor kafka group reader

0.6.11 (March 22th, 2017)
------------------------
* Support missing local cluster in config

0.6.10 (March 16th, 2017)
------------------------
* add generic prechecks in kafka-rolling-restart tool

0.6.9 (March 15th, 2017)
------------------------
* pin upper limit of kafka-python

0.6.8 (March 2nd, 2017)
------------------------
* Fixes terminate for expection cases in kafka-check

0.6.7 (March 2nd, 2017)
------------------------
* Optionally sort kafka-consumer-manager output by offset distance
* Support json output for kafka-checks

0.6.6 (March 1st, 2017)
-------------------------
* kafka-python>=1.3.2,<1.4.0 in setup.py
* 0.10 integration tests

0.6.5 (February 22, 2017)
-------------------------
* Fix list_topics flakiness in kafka-consumer-manager

0.6.4 (February 15, 2017)
-------------------------
* Upgrade kafka-python in use to 1.3.2
* Use new KafkaConsumer for KafkaGroupReader

0.6.3 (January 26, 2017)
------------------------
* Fix KafkaGroupreader when reading consumer group with partition zero.

0.6.2 (January 25, 2017)
------------------------
* Add storage option for a few kafka_consumer_manager subcommands
* Change default offset storage from zookeeper to kafka
* Autodetecting the number of partitions for the __commit_offsets topic

0.6.1 (December 15, 2016)
-------------------------
* Fix integration tests

0.6.0 (December 15, 2016)
-------------------------
* Refactor kafka-cluster-manager to support multiple balancer classes and metrics
* Add PartitionMeasurer class and --partition-measurer option for providing user partition metrics
* Add --genetic-balancer option to kafka-cluster-manager to make use of the genetic balancer
* Change kafka-cluster-manager stats command output to include user partition metrics
* Add --show-stats option to kafka-cluster-manager rebalance

0.5.7 (December 12, 2016)
------------------------
* Fetch group topics only from a single __consumer_offsets partition

0.5.6 (December 8, 2016)
------------------------
* Add offline partitions check for kafka-check

0.5.5 (November 15, 2016)
-------------------------
* Fix set_replication_factor command plan generation

0.5.4 (November 15, 2016)
-------------------------
* Fix offset_get when the group name is stored only in kafka
* Add offset_set retry when writing offsets to kafka

0.5.3 (November 4, 2016)
------------------------
* Fix a rebalance bug that would not generate a convergent assignment
* Check for pending asssignment before fetching the cluster topology
* Docs fixes

0.5.2 (November 1, 2016)
------------------------
* Add short options from cluster-type and cluster-name

0.5.1 (October 14, 2016)
------------------------
* Add option to see offset-distance for a consumer-group

0.5.0 (September 23, 2016)
--------------------------
* Add command set_replication_factor command
* Fix kafka-cluster-manager error on empty clusters

0.4.2 (September 2, 2016)
-------------------------
* Fix bug in cluster rebalance while updating sibling_distance

0.4.1 (September 1, 2016)
-------------------------
* Fix bug in cluster rebalance when replication group is None

0.4.0 (August 19, 2016)
-----------------------
* Add get topic watermark command
* Fix offset get json output

0.3.3 (July 29, 2016)
---------------------
* Fix bug in decommissioning of failed brokers

0.3.2 (July 14, 2016)
---------------------
* Make min_isr and under replicated partitions check much faster

0.3.1 (July 5, 2016)
---------------------
* Use error field from metadata response in under replicated partition check
* Fix small typo in cluster manager logging

0.3.0 (July 1, 2016)
---------------------
* Refactor under replicated partition check to use metadata request
* Add minimum replica number parameter to under replicated check
* Fix cluster manager logging

0.2.1 (June 21, 2016)
---------------------
* Add verbose option to kafka-check

0.2.0 (June 15, 2016)
----------------------
* Add under replicated partition check
* Add log segment corruption check
* Fix decommission command bug that caused decommission to fail in some cases
* Fix config when HOME env variable is not defined

0.1.2 (June 8, 2016)
----------------------
* Fix bug for no available under-loaded brokers

0.1.1 (May 17, 2016)
----------------------

* Fix group-parser local import

0.1.0 (May 17, 2016)
----------------------

* Initial open-source release
