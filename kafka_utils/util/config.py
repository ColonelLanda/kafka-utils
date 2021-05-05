# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import absolute_import

import glob
import logging
import os
from collections import namedtuple
import ssl

import six
import yaml
from six.moves import map

from kafka_utils.util.error import ConfigurationError
from kafka_utils.util.error import InvalidConfigurationError
from kafka_utils.util.error import MissingConfigurationError


DEFAULT_KAFKA_TOPOLOGY_BASE_PATH = '/etc/kafka_discovery'
HOME_OVERRIDE = '.kafka_discovery'


class ClusterConfig(
    namedtuple(
        'ClusterConfig',
        ['type', 'name', 'broker_list', 'zookeeper', 'key_path', 'cert_path'],
    ),
):
    """Cluster configuration.
    :param name: cluster name
    :param broker_list: list of kafka brokers
    :param zookeeper: zookeeper connection string
    """

    def __ne__(self, other):
        return self.__hash__() != other.__hash__()

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

    def __hash__(self):
        if isinstance(self.broker_list, list):
            broker_list = self.broker_list
        else:
            broker_list = self.broker_list.split(',')
        zk_list = self.zookeeper.split(',')
        return hash((
            self.type,
            self.name,
            ",".join(sorted([_f for _f in broker_list if _f])),
            ",".join(sorted([_f for _f in zk_list if _f]))
        ))

    @property
    def ssl_parameters(self):
        if self.key_path:
            context = ssl.SSLContext()
            # the certificate used for authentication is self signed on the server side, which is why
            # we only validate the client on the server, the client does not validate the server's certificate
            context.verify_mode = ssl.CERT_NONE
            context.load_cert_chain(certfile=self.cert_path, keyfile=self.key_path)
            return {'security_protocol': 'SSL', 'ssl_context': context}
        return {}


def load_yaml_config(config_path):
    with open(config_path, 'r') as config_file:
        return yaml.safe_load(config_file)


class TopologyConfiguration(object):
    """Topology configuration for a kafka cluster.

    Read a cluster_type.yaml from the kafka_topology_path.
    Example config file:
    .. code-block:: yaml

       clusters:
         cluster1:
             broker_list:
               - "broker1:9092"
               - "broker2:9092"
             zookeeper: "zookeeper1:2181/mykafka"
         cluster2:
             broker_list:
               - "broker3:9092"
               - "broker4:9092"
             zookeeper: "zookeeper2:2181/mykafka"
       local_config:
         cluster: cluster1


    :param cluster_type: kafka cluster type.
    :type cluster_type: string
    :param kafka_topology_path: path of the directory containing
        the kafka topology.yaml config
    :type kafka_topology_path: string
    """

    def __init__(
        self,
        cluster_type,
        kafka_topology_path=DEFAULT_KAFKA_TOPOLOGY_BASE_PATH
    ):
        self.kafka_topology_path = kafka_topology_path
        self.cluster_type = cluster_type
        self.log = logging.getLogger(self.__class__.__name__)
        self.clusters = None
        self.local_config = None
        self.load_topology_config()

    def __eq__(self, other):
        if all([
            self.cluster_type == other.cluster_type,
            self.clusters == other.clusters,
            self.local_config == other.local_config,
        ]):
            return True
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def load_topology_config(self):
        """Load the topology configuration"""
        config_path = os.path.join(
            self.kafka_topology_path,
            '{id}.yaml'.format(id=self.cluster_type),
        )
        self.log.debug("Loading configuration from %s", config_path)
        if os.path.isfile(config_path):
            topology_config = load_yaml_config(config_path)
        else:
            raise MissingConfigurationError(
                "Topology configuration {0} for cluster {1} "
                "does not exist".format(
                    config_path,
                    self.cluster_type,
                )
            )
        self.log.debug("Topology configuration %s", topology_config)
        try:
            self.clusters = topology_config['clusters']
        except KeyError:
            self.log.exception("Invalid topology file")
            raise InvalidConfigurationError("Invalid topology file {0}".format(
                config_path))
        if 'local_config' in topology_config:
            self.local_config = topology_config['local_config']

    def get_all_clusters(self):
        return [
            self._config_to_cluster(name, cluster)
            for name, cluster in six.iteritems(self.clusters)
        ]

    def get_cluster_by_name(self, name):
        if name in self.clusters:
            cluster = self.clusters[name]
            return self._config_to_cluster(name, cluster)
        raise ConfigurationError("No cluster with name: {0}".format(name))

    def get_local_cluster(self):
        if self.local_config:
            try:
                local_cluster = self.clusters[self.local_config['cluster']]
                return self._config_to_cluster(self.local_config['cluster'], local_cluster)
            except KeyError:
                self.log.exception("Invalid topology file")
                raise InvalidConfigurationError("Invalid topology file")
        else:
            raise ConfigurationError("No default local cluster configured")

    def _config_to_cluster(self, name, cluster_conf):
        return ClusterConfig(
            type=self.cluster_type,
            name=name,
            broker_list=cluster_conf['broker_list'],
            zookeeper=cluster_conf['zookeeper'],
            cert_path=cluster_conf.get("cert_path"),
            key_path=cluster_conf.get("key_path"),
        )

    def __repr__(self):
        return ("TopologyConfig: cluster_type {0}, clusters: {1},"
                "local_config {2}".format(
                    self.cluster_type,
                    self.clusters,
                    self.local_config
                ))


def get_conf_dirs():
    config_dirs = []
    if os.environ.get("KAFKA_DISCOVERY_DIR"):
        config_dirs.append(os.environ["KAFKA_DISCOVERY_DIR"])
    if os.environ.get("HOME"):
        home_config = os.path.join(
            os.path.abspath(os.environ['HOME']),
            HOME_OVERRIDE,
        )
        if os.path.isdir(home_config):
            config_dirs.append(home_config)
    config_dirs.append(DEFAULT_KAFKA_TOPOLOGY_BASE_PATH)
    return config_dirs


def get_cluster_config(
    cluster_type,
    cluster_name=None,
    kafka_topology_base_path=None,
):
    """Return the cluster configuration.
    Use the local cluster if cluster_name is not specified.

    :param cluster_type: the type of the cluster
    :type cluster_type: string
    :param cluster_name: the name of the cluster
    :type cluster_name: string
    :param kafka_topology_base_path: base path to look for <cluster_type>.yaml
    :type cluster_name: string
    :returns: the cluster
    :rtype: ClusterConfig
    """
    if not kafka_topology_base_path:
        config_dirs = get_conf_dirs()
    else:
        config_dirs = [kafka_topology_base_path]

    topology = None
    for config_dir in config_dirs:
        try:
            topology = TopologyConfiguration(
                cluster_type,
                config_dir,
            )
        except MissingConfigurationError:
            pass
    if not topology:
        raise MissingConfigurationError(
            "No available configuration for type {0}".format(cluster_type),
        )

    if cluster_name:
        return topology.get_cluster_by_name(cluster_name)
    else:
        return topology.get_local_cluster()


def iter_configurations(kafka_topology_base_path=None):
    """Cluster topology iterator.
    Iterate over all the topologies available in config.
    """
    if not kafka_topology_base_path:
        config_dirs = get_conf_dirs()
    else:
        config_dirs = [kafka_topology_base_path]

    types = set()
    for config_dir in config_dirs:
        new_types = [x for x in map(
            lambda x: os.path.basename(x)[:-5],
            glob.glob('{0}/*.yaml'.format(config_dir)),
        ) if x not in types]
        for cluster_type in new_types:
            try:
                topology = TopologyConfiguration(
                    cluster_type,
                    config_dir,
                )
            except ConfigurationError:
                continue
            types.add(cluster_type)
            yield topology
