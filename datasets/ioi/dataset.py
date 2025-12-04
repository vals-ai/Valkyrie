"""
Extracts the IOI datset from a tar file, and breaks it down into tasks.

Example Tar usage:
tar -czf archive.tar.gz <directory>

Extract the tar file:
tar -xzf archive.tar.gz


"""

import io
from pathlib import Path, PurePosixPath
from typing import override

from model_library.base import TextInput
from agentic_harness.base.dataset import Dataset
from agentic_harness.base.types import Task, TaskGroup
from vals import Suite, Test
import PyPDF2
import tarfile


class IOIDataset(Dataset):
    _TAR_PATH: Path = Path("datasets/ioi/files/archive.tar.gz")
    _TAR_EXAM_PATH: str = "exams"
    _SYSTEM_PROMPT = """
        You will solve programming problems from the IOI competition.

        Provide all submissions and execution requests in a c++ (v20) program with stdlib imports. Submit all code enclosed with three backticks. For example:

        ```
        [your code here]
        ```

        When you would like to make a submission, use the appropriate tool call. After you make a submission, you will see the score you received for each of the subtasks of that problem.

        You may make at most 50 submissions in at most 100 turns. Your score will be calculated based on the number of categories passed acorss all submissions.

        When you are done working and would not like to make more submissions you should respond with 'EXIT'.

        The following includes the problem statement and any relevant contextual files:
        {question}
        """

    @staticmethod
    def _extract_text_from_pdf(pdf_bytes: bytes) -> str:
        """Ripped from https://github.com/vals-ai/ioi-agent/blob/main/utils.py"""
        text = ""
        with io.BytesIO(pdf_bytes) as f:
            reader = PyPDF2.PdfReader(f)
            for page in reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"

        return text.strip()

    async def _pull_tests(self, suite_id: str) -> list[Test]:
        """
        Pulls all tests from a single suite.
        """
        tests: list[Test]

        suite = await Suite.from_id(suite_id)
        tests = suite.tests

        return tests

    def _fetch_file_text(self, member: tarfile.TarInfo, raw_bytes: bytes) -> str:
        if member.name.lower().endswith(".pdf"):
            return self._extract_text_from_pdf(raw_bytes)
        else:
            return raw_bytes.decode("utf-8", errors="ignore")

    def _safe_read_file(self, member: tarfile.TarInfo, tar: tarfile.TarFile) -> bytes:
        f = tar.extractfile(member)
        if f is None:
            raise ValueError("Unable to read file, extracted file was None")

        return f.read()

    def _fetch_question_text(self, targz_bytes: bytes, question_path: str) -> str:
        result: list[str] = []

        with tarfile.open(fileobj=io.BytesIO(targz_bytes), mode="r:gz") as tar:
            members = [
                m
                for m in tar.getmembers()
                # Exact match ex. `messages/``
                if m.isfile() and m.name.startswith(question_path + "/")
            ]

            for member in sorted(members, key=lambda m: m.name):
                relative_path = PurePosixPath(member.name)

                raw_bytes = self._safe_read_file(member, tar)

                try:
                    file_contents = self._fetch_file_text(member, raw_bytes)
                except Exception as e:
                    raise ValueError(f"Error fetching file text: {e}")

                result.append(f"[{relative_path}]")
                result.append(file_contents)
                result.append("\n")

        return "\n".join(result)

    def _load_tar_file_bytes(self, tar_path: Path) -> bytes:
        with open(tar_path, "rb") as f:
            data = f.read()

            return data

    def _format_question(self, test: Test, question_text: str) -> TaskGroup:
        """Formats a question and returns a single task group"""
        formatted_system_prompt = self._SYSTEM_PROMPT.format(question=question_text)
        input = [TextInput(text=formatted_system_prompt)]

        task = Task(id=test.id or "", input=input)

        return TaskGroup(tasks=[task])

    def _path_exists_in_tar(self, tar_bytes: bytes, path: str) -> bool:
        """We pass in a relative path like `2024/nile` and we check if the dir exists"""
        with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:gz") as tar:
            members = [m for m in tar.getmembers() if m.isdir()]

            return any(m.name == path for m in members)

    async def _create_task_groups(self, suite_id: str) -> list[TaskGroup]:
        """Creates a task group for a given task"""
        raw_tests = await self._pull_tests(suite_id)

        tar_bytes = self._load_tar_file_bytes(self._TAR_PATH)

        task_groups: list[TaskGroup] = []
        for test in raw_tests:
            expected_path = f"{self._TAR_EXAM_PATH}/{test.input_under_test}"
            if not self._path_exists_in_tar(tar_bytes, expected_path):
                raise ValueError(f"Path {expected_path} does not exist in tar file")

            question_text = self._fetch_question_text(tar_bytes, expected_path)
            task_groups.append(self._format_question(test, question_text))

        return task_groups

    @override
    async def create(self) -> list[TaskGroup]:
        """
        Creates the final task group list.
        1. Pulls all of the tests from all of the task suites.
        2. Creates a task group for each task inside of the config using the coupled information.
        3. Returns a list of the final task groups.

        NOTE: We are coupling all datasets inside of IOI since the format is the same and its like 15-20 questions combined
        """
        task_groups: list[TaskGroup] = []
        suite_id = self._config.get("suite_id")

        if suite_id is None:
            raise ValueError("`dataset.suite_id` is required")

        task_groups.extend(await self._create_task_groups(suite_id))

        return task_groups
