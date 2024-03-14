# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.


import logging

import requests
from requests.auth import HTTPBasicAuth
from retry.api import retry_call  # type: ignore

from test_workflow.benchmark_test.benchmark_args import BenchmarkArgs
from test_workflow.integ_test.utils import get_password


class BenchmarkTestCluster:
    args: BenchmarkArgs
    cluster_endpoint_with_port: str

    def __init__(
            self,
            args: BenchmarkArgs

    ) -> None:
        self.args = args
        self.cluster_endpoint_with_port = None

    def start(self) -> None:
        self.set_distribution_version(self.args.distribution_version)  # add curl command changes
        self.wait_for_processing()
        self.cluster_endpoint_with_port = "".join([self.args.cluster_endpoint, ":", str(self.port)])

    @property
    def endpoint(self) -> str:
        return self.args.cluster_endpoint

    @property
    def endpoint_with_port(self) -> str:
        return self.cluster_endpoint_with_port

    @property
    def port(self) -> int:
        return 80 if self.args.insecure else 443

    @property
    def version(self) -> str:
        return self.args.distribution_version

    def wait_for_processing(self, tries: int = 3, delay: int = 15, backoff: int = 2) -> None:

        logging.info(f"Waiting for domain at {self.endpoint} to be up")
        protocol = "http://" if self.args.insecure else "https://"
        url = "".join([protocol, self.endpoint, "/_cluster/health"])
        logging.info(url)
        request_args = {"url": url} if self.args.insecure else {"url": url, "auth": HTTPBasicAuth("admin", get_password(str(self.args.distribution_version))),  # type: ignore
                                                                "verify": False}  # type: ignore
        logging.info(request_args)
        retry_call(requests.get, fkwargs=request_args, tries=tries, delay=delay, backoff=backoff)

    def set_distribution_version(self, version: str) -> None:
        self.args.distribution_version = version
