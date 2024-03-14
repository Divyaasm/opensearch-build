# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.


import json
import logging
import os
import subprocess
from contextlib import contextmanager
from typing import Any, Generator, Union

import requests
import semver
from requests.auth import HTTPBasicAuth
from retry.api import retry_call  # type: ignore

from manifests.build_manifest import BuildManifest
from manifests.bundle_manifest import BundleManifest
from test_workflow.benchmark_test.benchmark_args import BenchmarkArgs


class BenchmarkTestCluster:
    manifest: Union[BundleManifest, BuildManifest]
    current_workspace: str
    args: BenchmarkArgs
    output_file: str
    params: str
    cluster_endpoint_with_port: str

    """
    Represents a performance test cluster. This class deploys the opensearch bundle with CDK. Supports both single
    and multi-node clusters
    """

    def __init__(
            self,
            bundle_manifest: Union[BundleManifest, BuildManifest],
            config: dict,
            args: BenchmarkArgs,
            current_workspace: str
    ) -> None:
        self.manifest = bundle_manifest
        self.current_workspace = current_workspace
        self.args = args

        if self.args.cluster_endpoint is None:

            role = config["Constants"]["Role"]
            logging.info(role)
            params_dict = self.setup_cdk_params(config)
            params_list = []
            for key, value in params_dict.items():
                if value:
                    '''
                    TODO: To send json input to typescript code from command line it needs to be enclosed in
                    single-quotes, this is a temp fix to achieve that since the quoted string passed from command line in
                    tesh.sh wrapper script gets un-quoted and we need to handle it here.
                    '''
                    if key == 'additionalConfig':
                        params_list.append(f" -c {key}=\'{value}\'")
                    else:
                        params_list.append(f" -c {key}={value}")
            role_params = (
                " --require-approval=never"
            )
            self.params = "".join(params_list) + role_params

            self.cluster_endpoint_with_port = None
            self.output_file = "output.json"
            self.stack_name = f"opensearch-infra-stack-{self.args.stack_suffix}"
            if self.manifest:
                self.stack_name += f"-{self.manifest.build.id}-{self.manifest.build.architecture}"

    def start(self) -> None:
        if self.args.cluster_endpoint is None:
            command = f"npm install && cdk deploy \"*\" {self.params} --outputs-file {self.output_file}"

            logging.info(f'Executing "{command}" in {os.getcwd()}')
            subprocess.check_call(command, cwd=os.getcwd(), shell=True)
            with open(self.output_file, "r") as read_file:
                load_output = json.load(read_file)
                self.create_endpoint(load_output)

        self.wait_for_processing()
        self.cluster_endpoint_with_port = "".join([self.args.cluster_endpoint, ":", str(self.port)])

    def create_endpoint(self, cdk_output: dict) -> None:
        self.args.cluster_endpoint = cdk_output[self.stack_name].get('loadbalancerurl', None)
        if self.args.cluster_endpoint is None:
            raise RuntimeError("Unable to fetch the cluster endpoint from cdk output")

    @property
    def endpoint(self) -> str:
        return self.args.cluster_endpoint

    @property
    def endpoint_with_port(self) -> str:
        return self.cluster_endpoint_with_port

    @property
    def port(self) -> int:
        return 80 if self.args.insecure else 443

    def terminate(self) -> None:
        command = f"cdk destroy {self.stack_name} {self.params} --force"
        logging.info(f'Executing "{command}" in {os.getcwd()}')

        subprocess.check_call(command, cwd=os.getcwd(), shell=True)

    def wait_for_processing(self, tries: int = 3, delay: int = 15, backoff: int = 2) -> None:
        # To-do: Make this better
        password = 'admin'
        if self.manifest:
            if semver.compare(self.manifest.build.version, '2.12.0') != -1:
                password = 'myStrongPassword123!'
        else:
            if semver.compare(self.args.distribution_version, '2.12.0') != -1:
                password = 'myStrongPassword123!'

        logging.info(f"Waiting for domain at {self.endpoint} to be up")
        protocol = "http://" if self.args.insecure else "https://"
        url = "".join([protocol, self.endpoint, "/_cluster/health"])
        request_args = {"url": url} if self.args.insecure else {"url": url, "auth": HTTPBasicAuth("admin", password),  # type: ignore
                                                                "verify": False}  # type: ignore
        retry_call(requests.get, fkwargs=request_args, tries=tries, delay=delay, backoff=backoff)

    def setup_cdk_params(self, config: dict) -> dict:
        suffix = ''
        need_strong_password = False
        if self.args.stack_suffix and self.manifest:
            suffix = self.args.stack_suffix + '-' + self.manifest.build.id + '-' + self.manifest.build.architecture
        elif self.manifest:
            suffix = self.manifest.build.id + '-' + self.manifest.build.architecture
        elif self.args.stack_suffix:
            suffix = self.args.stack_suffix

        if self.manifest:
            artifact_url = self.manifest.build.location if isinstance(self.manifest, BundleManifest) else \
                f"https://artifacts.opensearch.org/snapshots/core/opensearch/{self.manifest.build.version}/opensearch-min-" \
                f"{self.manifest.build.version}-linux-{self.manifest.build.architecture}-latest.tar.gz"
            if not self.args.insecure and semver.compare(self.manifest.build.version, '2.12.0') != -1:
                need_strong_password = True
        else:
            artifact_url = self.args.distribution_url.strip()
            if not self.args.insecure and semver.compare(self.args.distribution_version, '2.12.0') != -1:
                need_strong_password = True

        return {
            "distributionUrl": artifact_url,
            "vpcId": config["Constants"]["VpcId"],
            "account": config["Constants"]["AccountId"],
            "region": config["Constants"]["Region"],
            "suffix": suffix,
            "securityDisabled": str(self.args.insecure).lower(),
            "adminPassword": 'myStrongPassword123!' if need_strong_password else None,
            "cpuArch": self.manifest.build.architecture if self.manifest else 'x64',
            "singleNodeCluster": str(self.args.single_node).lower(),
            "distVersion": self.manifest.build.version if self.manifest else self.args.distribution_version,
            "minDistribution": str(self.args.min_distribution).lower(),
            "serverAccessType": config["Constants"]["serverAccessType"],
            "restrictServerAccessTo": config["Constants"]["restrictServerAccessTo"],
            "additionalConfig": self.args.additional_config,
            "dataInstanceType": self.args.data_instance_type,
            "managerNodeCount": self.args.manager_node_count,
            "dataNodeCount": self.args.data_node_count,
            "clientNodeCount": self.args.client_node_count,
            "ingestNodeCount": self.args.ingest_node_count,
            "mlNodeCount": self.args.ml_node_count,
            "dataNodeStorage": self.args.data_node_storage,
            "mlNodeStorage": self.args.ml_node_storage,
            "jvmSysProps": self.args.jvm_sys_props,
            "use50PercentHeap": str(self.args.use_50_percent_heap).lower(),
            "isInternal": config["Constants"]["isInternal"],
            "enableRemoteStore": str(self.args.enable_remote_store).lower(),
            "customRoleArn": config["Constants"]["IamRoleArn"]
        }

    @classmethod
    @contextmanager
    def create(cls, bundle_manifest: Union[BundleManifest, BuildManifest], config: dict, args: BenchmarkArgs, current_workspace: str) -> Generator[Any, None, None]:
        """
        Set up the cluster. When this method returns, the cluster must be available to take requests.
        Throws ClusterCreationException if the cluster could not start for some reason. If this exception is thrown, the caller does not need to call "destroy".
        """
        cluster = cls(bundle_manifest, config, args, current_workspace)
        try:
            destroy_cluster = args.cluster_endpoint
            logging.info(destroy_cluster)
            cluster.start()
            yield cluster
        finally:
            logging.info("out")
            if not destroy_cluster:
                logging.info("true")
                cluster.terminate()
