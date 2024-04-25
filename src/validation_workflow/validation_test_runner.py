# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# type: ignore

from system.temporary_directory import TemporaryDirectory

from validation_workflow.deb.validation_deb import ValidateDeb
from validation_workflow.docker.validation_docker import ValidateDocker
from validation_workflow.rpm.validation_rpm import ValidateRpm
from validation_workflow.tar.validation_tar import ValidateTar
from validation_workflow.validation import Validation
from validation_workflow.validation_args import ValidationArgs
from validation_workflow.yum.validation_yum import ValidateYum
from validation_workflow.zip.validation_zip import ValidateZip


class ValidationTestRunner:
    RUNNERS = {
        "docker": ValidateDocker,
        "tar": ValidateTar,
        "rpm": ValidateRpm,
        "yum": ValidateYum,
        "zip": ValidateZip,
        "deb": ValidateDeb
    }

    @classmethod
    def dispatch(cls, args: ValidationArgs, dist: str) -> Validation:
        with TemporaryDirectory(keep=False) as work_dir:
            print("Temporary directory created:", work_dir)
            obj = cls.RUNNERS[dist](args, work_dir)
            print("Object created:", obj)
            return obj
