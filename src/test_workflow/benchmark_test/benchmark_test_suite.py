# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

import pandas as pd
import json
import logging
import os
import subprocess
from typing import Any

from test_workflow.benchmark_test.benchmark_args import BenchmarkArgs
from system.temporary_directory import TemporaryDirectory


class BenchmarkTestSuite:
    endpoint: str
    security: bool
    args: BenchmarkArgs
    command: str
    password: str

    """
    Represents a performance test suite. This class runs rally test on the deployed cluster with the provided IP.
    """

    def __exit__(self, exc_type: Any, exc_value: Any, exc_traceback: Any) -> None:
        self.tmp_dir.__exit__(exc_type, exc_value, exc_traceback)

    def __init__(
            self,
            endpoint: Any,
            security: bool,
            args: BenchmarkArgs,
            password: str
    ) -> None:
        self.endpoint = endpoint
        self.security = security
        self.args = args
        self.password = password
        self.tmp_dir = TemporaryDirectory(keep=keep)

        # Pass the cluster endpoints with -t for multi-cluster use cases(e.g. cross-cluster-replication)
        self.container_name = f'docker-container-{args.stack_suffix}'  # container name
        self.command = f'docker run --name {self.container_name}'
        if self.args.benchmark_config:
            self.command += f" -it -v {args.benchmark_config}:/opensearch-benchmark/.benchmark/benchmark.ini"
        self.command += f" opensearchproject/opensearch-benchmark:latest execute-test --workload={self.args.workload} " \
                        f"--pipeline=benchmark-only --target-hosts={endpoint} --test-mode"

        if self.args.workload_params:
            logging.info(f"Workload Params are {args.workload_params}")
            self.command += f" --workload-params '{args.workload_params}'"

        if self.args.test_procedure:
            self.command += f" --test-procedure=\"{self.args.test_procedure}\""

        if self.args.exclude_tasks:
            self.command += f" --exclude-tasks=\"{self.args.exclude_tasks}\""

        if self.args.include_tasks:
            self.command += f" --include-tasks=\"{self.args.include_tasks}\""

        if self.args.user_tag:
            user_tag = f"--user-tag=\"{args.user_tag}\""
            self.command += f" {user_tag}"

        if self.args.telemetry:
            self.command += " --telemetry "
            for value in self.args.telemetry:
                self.command += f"{value},"
            if self.args.telemetry_params:
                self.command += f" --telemetry-params '{self.args.telemetry_params}'"

    def execute(self) -> None:
        if self.security:
            self.command += f' --client-options="timeout:300,use_ssl:true,verify_certs:false,basic_auth_user:\'{self.args.username}\',basic_auth_password:\'{self.password}\'"'
        else:
            self.command += ' --client-options="timeout:300"'
        log_info = f"Executing {self.command.replace(self.endpoint, len(self.endpoint) * '*').replace(self.args.username, len(self.args.username) * '*')}"
        logging.info(log_info.replace(self.password, len(self.password) * '*') if self.password else log_info)
        subprocess.check_call(f"{self.command}", cwd=os.getcwd(), shell=True)

        subprocess.check_call(f"docker start {self.container_name}", cwd=os.getcwd(), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        path = subprocess.check_output(f"docker exec {self.container_name} find /opensearch-benchmark -name test_execution.json", cwd=os.getcwd(), shell=True)
        subprocess.check_call(f"docker cp {self.container_name}:{path.decode().strip()} .", cwd=os.getcwd(), shell=True)
        file_path = os.path.join(os.getcwd(), "test_execution.json")
        self.convert(file_path)
        subprocess.check_call(f"docker stop {self.container_name}", cwd=os.getcwd(), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        subprocess.check_call(f"docker rm {self.container_name}", cwd=os.getcwd(), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def convert(self, results: str) -> None:
        with open(results) as file:
            data = json.load(file)
        formatted_data = pd.json_normalize(data["results"]["op_metrics"])
        formatted_data.to_csv("test_execution.csv", index=False)
        df = pd.read_csv(os.path.join(os.getcwd(), "test_execution.csv"))
        logging.info(df)