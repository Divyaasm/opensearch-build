# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

import logging
import os

from system.execute import execute
from system.process import Process
from system.temporary_directory import TemporaryDirectory
from test_workflow.integ_test.utils import get_password
from validation_workflow.api_test_cases import ApiTestCases
from validation_workflow.download_utils import DownloadUtils
from validation_workflow.validation import Validation
from validation_workflow.validation_args import ValidationArgs


class ValidateTar(Validation, DownloadUtils):

    def __init__(self, args: ValidationArgs, tmp_dir: TemporaryDirectory) -> None:
        super().__init__(args, tmp_dir)
        self.os_process = Process()
        self.osd_process = Process()

    def installation(self) -> bool:
        try:
            for project in self.args.projects:
                try:
                    self.filename = os.path.basename(self.args.file_path.get(project))
                    execute('mkdir ' + os.path.join(self.tmp_dir.path, project) + ' | tar -xzf ' + os.path.join(str(self.tmp_dir.path), self.filename) + ' -C ' + os.path.join(self.tmp_dir.path, project) + ' --strip-components=1', ".", True, False)  # noqa: E501
                    execute('ls', os.path.join(self.tmp_dir.path, "opensearch"))
                    if self.args.validate_native_plugin:
                        self.install_native_plugin("opensearch")
                    # execute(f'yes | ./bin/opensearch-plugin install {i}', os.path.join(str(self.tmp_dir.path), "opensearch"), check=True)

                except:
                    return False
        except:
            raise Exception('Failed to install Opensearch')
        return True

    def start_cluster(self) -> bool:
        try:
            self.os_process.start(f'export OPENSEARCH_INITIAL_ADMIN_PASSWORD={get_password(str(self.args.version))} && ./opensearch-tar-install.sh', os.path.join(self.tmp_dir.path, "opensearch"))
            if ("opensearch-dashboards" in self.args.projects):
                self.osd_process.start(os.path.join(str(self.tmp_dir.path), "opensearch-dashboards", "bin", "opensearch-dashboards"), ".")
            logging.info('Started cluster')
        except:
            raise Exception('Failed to Start Cluster')
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

    def cleanup(self) -> bool:
        try:
            self.os_process.terminate()
            if ("opensearch-dashboards" in self.args.projects):
                self.osd_process.terminate()
        except:
            raise Exception('Failed to terminate the processes that started OpenSearch and OpenSearch-Dashboards')
        return True

    # Manifest Usage:
    # The format for schema version 1.1 is:
    #         schema-version: "1.1"
    #         build:
    #           name: string
    #           version: string
    #           platform: linux, darwin or windows
    #           architecture: x64 or arm64
    #           distribution: tar, zip, deb and rpm
    #           id: build id
    #           location: /relative/path/to/tarball
    #         components:
    #           - name: string
    #             repository: URL of git repository
    #             ref: git ref that was built (sha, branch, or tag)
    #             commit_id: The actual git commit ID that was built (i.e. the resolved "ref")
    #             location: /relative/path/to/artifact
    # self.bundle_manifest = BundleManifest.from_path(self.BUNDLE_MANIFEST)
    # component = self.bundle_manifest.components[OpenSearch].commit_id

    # api request https://api.github.com/repos/opensearch-project/OpenSearch/contents/plugins?ref=59043524466b9d3354bd43bb21b7d32ce4c7c311
