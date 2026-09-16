# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

import glob
import logging
import os

from system.process import Process
from system.temporary_directory import TemporaryDirectory
from system.zip_file import ZipFile
from test_workflow.integ_test.utils import get_password
from validation_workflow.api_test_cases import ApiTestCases
from validation_workflow.download_utils import DownloadUtils
from validation_workflow.validation import Validation
from validation_workflow.validation_args import ValidationArgs


class ValidateZip(Validation, DownloadUtils):
    def __init__(self, args: ValidationArgs, tmp_dir: TemporaryDirectory) -> None:
        super().__init__(args, tmp_dir)
        self.os_process = Process()
        self.osd_process = Process()

    def installation(self) -> bool:
        try:
            for project in self.args.projects:
                with ZipFile(os.path.join(self.tmp_dir.path, os.path.basename(self.args.file_path.get(project))), "r") as zip:
                    zip.extractall(self.tmp_dir.path)
                if self.args.skip_core_plugins is None or self.args.skip_core_plugins:
                    installed_plugins_list = os.listdir(os.path.join(self.tmp_dir.path, f"opensearch-{self.args.version}", "plugins"))
                    self.install_native_plugin(os.path.join(self.tmp_dir.path, f"opensearch-{self.args.version}"), installed_plugins_list)
        except:
            raise Exception("Failed to install OpenSearch/OpenSearch-Dashboards")
        return True

    def start_cluster(self) -> bool:
        try:
            self.os_process.start(f"env OPENSEARCH_INITIAL_ADMIN_PASSWORD={get_password(str(self.args.version))} .\\opensearch-windows-install.bat",
                                  os.path.join(self.tmp_dir.path, f"opensearch-{self.args.version}"), False)
            if "opensearch-dashboards" in self.args.projects:
                self.osd_process.start(".\\bin\\opensearch-dashboards.bat", os.path.join(self.tmp_dir.path, f"opensearch-dashboards-{self.args.version}"), False)
            logging.info("Starting cluster")
        except Exception as e:
            # Surface any captured process output before failing so the real
            # cause (bad launcher path, immediate crash, etc.) is visible.
            try:
                self.dump_cluster_logs()
            except Exception:
                pass
            raise Exception(f'Failed to Start Cluster: {str(e)}')
        return True

    def validation(self) -> bool:
        if self.check_cluster_readiness():
            test_result, counter = ApiTestCases().test_apis(self.args.version, self.args.projects,
                                                            self.check_for_security_plugin(os.path.join(self.tmp_dir.path, "opensearch")) if self.args.allow_http else True)
            if (test_result):
                logging.info(f'All tests Pass : {counter}')
                return True
            else:
                self.cleanup()
                raise Exception(f'Not all tests Pass : {counter}')
        else:
            self.cleanup()
            raise Exception("Cluster is not ready for API test")

    def dump_cluster_logs(self) -> None:
        # 1. Dump the captured stdout/stderr from the launched processes.
        try:
            self.os_process.log_output(label="opensearch")
        except Exception as e:
            logging.warning(f"Could not dump opensearch process output: {e}")

        if "opensearch-dashboards" in self.args.projects:
            try:
                self.osd_process.log_output(label="opensearch-dashboards")
            except Exception as e:
                logging.warning(f"Could not dump opensearch-dashboards process output: {e}")

        # 2. Tail the OpenSearch log files written to disk (the most useful
        #    source for boot failures such as bad config, bootstrap checks,
        #    JVM/heap errors, plugin load failures, or security cert issues).
        os_log_dir = os.path.join(self.tmp_dir.path, f"opensearch-{self.args.version}", "logs")
        self._tail_logs(os_log_dir)

        if "opensearch-dashboards" in self.args.projects:
            osd_log_dir = os.path.join(self.tmp_dir.path, f"opensearch-dashboards-{self.args.version}", "logs")
            self._tail_logs(osd_log_dir)

    def _tail_logs(self, log_dir: str, num_lines: int = 100) -> None:
        logging.info(f"Scanning for cluster log files in: {log_dir}")
        if not os.path.isdir(log_dir):
            logging.warning(f"Log directory not found: {log_dir} "
                            f"(cluster likely died before creating it; see process stdout/stderr above)")
            return
        log_files = glob.glob(os.path.join(log_dir, "*.log"))
        if not log_files:
            logging.warning(f"No .log files found in: {log_dir}")
            return
        for log_file in log_files:
            try:
                with open(log_file, "r", encoding="utf-8", errors="replace") as f:
                    lines = f.readlines()
                tail = "".join(lines[-num_lines:])
                logging.info(
                    f"\n===== {os.path.basename(log_file)} (last {num_lines} lines) BEGIN =====\n"
                    f"{tail}\n"
                    f"===== {os.path.basename(log_file)} END ====="
                )
            except Exception as e:
                logging.warning(f"Could not read log file {log_file}: {e}")

    def cleanup(self) -> bool:
        try:
            self.os_process.terminate()
            if ("opensearch-dashboards" in self.args.projects):
                self.osd_process.terminate()
        except:
            raise Exception('Failed to terminate the processes that started OpenSearch and OpenSearch-Dashboards')
        return True
