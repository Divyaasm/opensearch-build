# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
import logging
import os
import subprocess
import tempfile
from typing import Any

import psutil


class Process:
    def __init__(self) -> None:
        self.process: subprocess.Popen[bytes] = None
        self.require_sudo: bool = False
        self.stdout: Any = None
        self.stderr: Any = None
        self.__stdout_data__: str = None
        self.__stderr_data__: str = None

    def start(self, command: str, cwd: str, require_sudo: bool = False) -> None:
        if self.started:
            raise ProcessStartedError(self.pid)

        self.stdout = tempfile.NamedTemporaryFile(mode="r+", delete=False, encoding='utf-8')
        self.stderr = tempfile.NamedTemporaryFile(mode="r+", delete=False, encoding='utf-8')

        self.require_sudo = require_sudo

        self.process = subprocess.Popen(
            command,
            cwd=cwd,
            shell=True,
            stdout=self.stdout,
            stderr=self.stderr,
        )

    def log_output(self, label: str = "process") -> None:
        """
        Read and log the currently captured stdout/stderr of a running (or
        finished) process WITHOUT terminating it. Safe to call at any time.
        This surfaces the process output to the console/Jenkins log so that
        startup failures are visible even when the process never became ready.
        """
        # Report whether the launched process has already exited. A non-None
        # exit code means the launcher (e.g. opensearch-windows-install.bat)
        # died early -- before any logs/*.log file could be written -- which is
        # exactly the case we want visibility into.
        exit_code = self.exit_code()
        if exit_code is None:
            logging.info(f"{label} is still running (pid {self.pid}); dumping output captured so far.")
        else:
            logging.error(f"{label} has already exited with code {exit_code}; it failed to stay up.")

        for stream_name, stream in (("stdout", self.stdout), ("stderr", self.stderr)):
            if stream is None:
                # Process already terminated; fall back to cached data.
                cached = self.__stdout_data__ if stream_name == "stdout" else self.__stderr_data__
                if cached and cached.strip():
                    logging.info(
                        f"\n===== {label} {stream_name} (cached) BEGIN =====\n"
                        f"{cached}\n"
                        f"===== {label} {stream_name} END ====="
                    )
                continue
            try:
                # Flush any OS-level buffered writes from the child process so
                # we read the most complete output available, then read from
                # the start without disrupting ongoing writes.
                try:
                    stream.flush()
                    os.fsync(stream.fileno())
                except Exception:
                    pass
                current_pos = stream.tell()
                stream.seek(0)
                data = stream.read()
                stream.seek(current_pos)
            except Exception as e:
                logging.warning(f"Could not read {stream_name} for {label}: {e}")
                continue

            if data and data.strip():
                logging.info(
                    f"\n===== {label} {stream_name} (captured) BEGIN =====\n"
                    f"{data}\n"
                    f"===== {label} {stream_name} END ====="
                )
            else:
                logging.info(f"{label} {stream_name} is empty (no output captured yet)")

    def exit_code(self) -> Any:
        """
        Return the process exit code if it has already terminated, or None if
        it is still running. Returns None if the process was never started.
        """
        if not self.started:
            return None
        return self.process.poll()

    def terminate(self) -> int:
        if not self.started:
            raise ProcessNotStartedError()

        parent = psutil.Process(self.process.pid)
        logging.debug("Checking for child processes")
        child_processes = parent.children(recursive=True)
        for child in child_processes:
            logging.debug(f"Found child process with pid {child.pid}")
            if child.pid != self.process.pid:
                logging.debug(f"Sending SIGKILL to {child.pid} ")
                child.kill() if self.require_sudo is False else subprocess.check_call(f"sudo kill -9 {child.pid}", shell=True)
        logging.info(f"Sending SIGKILL to PID {self.process.pid}")

        self.process.kill() if self.require_sudo is False else subprocess.check_call(f"sudo kill -9 {self.process.pid}", shell=True)

        logging.info(f"Process killed with exit code {self.process.returncode}")

        if self.stdout:
            self.stdout.seek(0)
            self.__stdout_data__ = self.stdout.read()
            self.stdout.flush()
            self.stdout.close()
            for proc in psutil.process_iter():
                try:
                    for item in proc.open_files():
                        if self.stdout.name == item.path:
                            logging.error(f"stdout {item} is being used by process {proc}")
                except Exception:
                    pass
            os.unlink(self.stdout.name)
            self.stdout = None

        if self.stderr:
            self.stderr.seek(0)
            self.__stderr_data__ = self.stderr.read()
            self.stderr.flush()
            self.stderr.close()
            for proc in psutil.process_iter():
                try:
                    for item in proc.open_files():
                        if self.stderr.name == item.path:
                            logging.error(f"stderr {item} is being used by process {proc}")
                except Exception:
                    pass
            os.unlink(self.stderr.name)
            self.stderr = None

        self.return_code = self.process.returncode
        self.process = None

        return self.return_code

    @property
    def started(self) -> bool:
        return True if self.process else False

    @property
    def pid(self) -> int:
        return self.process.pid if self.started else None

    @property
    def stdout_data(self) -> Any:
        return self.stdout.read() if self.stdout else self.__stdout_data__

    @property
    def stderr_data(self) -> Any:
        return self.stderr.read() if self.stderr else self.__stderr_data__


class ProcessStartedError(Exception):
    """
    Indicates that process already started.
    """

    def __init__(self, pid: int) -> None:
        self.pid = pid
        super().__init__(f"Process already started, pid: {pid}")


class ProcessNotStartedError(Exception):
    """
    Indicates that process has not started.
    """

    def __init__(self) -> None:
        super().__init__("Process has not started")
