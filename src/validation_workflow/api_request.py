#!/usr/bin/env python
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

from typing import Any
from test_workflow.integ_test.utils.version_utils import get_password

import logging
import requests


"""
This class is to run API test againt on local OpenSearch API URL with default port 9200.
It returns response status code and the response content.
"""


class ApiTest:

    def __init__(self, request_url: str) -> None:
        self.request_url = request_url
        self.apiHeaders_auth = {"Authorization": "Basic YWRtaW46bXlTdHJvbmdQYXNzd29yZDEyMyE="}  # default user/pass "admin/myStrongPassword123!" in Base64 format
        self.apiHeaders_accept = {"Accept": "*/*"}
        self.apiHeaders_content_type = {"Content-Type": "application/json"}
        self.apiHeaders = {}
        self.apiHeaders.update(self.apiHeaders_auth)
        self.apiHeaders.update(self.apiHeaders_accept)
        self.apiHeaders.update(self.apiHeaders_content_type)

    def api_get(self) -> Any:
        password = 'myStrongPassword123!' if get_password() == 'myStrongPassword123!' else None
        logging.info("password:" + password)

        response = requests.get(self.request_url, headers=self.apiHeaders, verify=False)
        return response.status_code, response.text
