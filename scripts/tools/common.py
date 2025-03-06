import os
import subprocess, shlex
from typing import Tuple

CallResult = Tuple[bool, str]

def sys_call(command, working_dir=None, use_pipe=False) -> CallResult:
  stdout = subprocess.PIPE if use_pipe else None
  process = subprocess.Popen(command, shell=True, cwd=working_dir, stdout=stdout, stderr=stdout)
  rc = process.wait()
  return (rc == 0, process.communicate()[0] if use_pipe else None)

def collect_files_by_ext(input_path: str, allowed_extensions: [str], files_to_skip: [str]):
# collect files for a check
  files = []
  for root, _, filenames in os.walk(input_path):
    for filename in filenames:
      _, ext = os.path.splitext(filename)
      if ext in allowed_extensions and filename not in files_to_skip:
        files.append(os.path.join(root, filename))

  return files