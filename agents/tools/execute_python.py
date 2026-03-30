"""
Tool: execute_python
Writes a Python script to disk and executes it, capturing stdout/stderr.
The script is saved under runs/{run_id}/generated_scripts/ for auditability.
"""
import subprocess
import time
from pathlib import Path

from langchain_core.tools import tool


def make_execute_python_tool(run_dir: str):
    """
    Factory that returns a tool bound to the current run's directory.
    """
    scripts_dir = Path(run_dir) / "generated_scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)

    @tool
    def execute_python(script_content: str, script_name: str = "") -> dict:
        """
        Write script_content to disk as a .py file and execute it with python3.
        Returns a dict with stdout, stderr, returncode, and the script path.
        The script is saved permanently under the run's generated_scripts directory.

        Args:
            script_content: The full Python script to execute.
            script_name: Optional name for the script file (without .py). Defaults to timestamped name.
        """
        if not script_name:
            script_name = f"script_{int(time.time())}"
        if not script_name.endswith(".py"):
            script_name = script_name + ".py"

        script_path = scripts_dir / script_name
        script_path.write_text(script_content)

        result = subprocess.run(
            ["python3", str(script_path)],
            capture_output=True,
            text=True,
            timeout=600,
        )

        success = result.returncode == 0
        return {
            "success": success,
            "returncode": result.returncode,
            "stdout": result.stdout[-8000:] if len(result.stdout) > 8000 else result.stdout,
            "stderr": result.stderr[-4000:] if len(result.stderr) > 4000 else result.stderr,
            "script_path": str(script_path),
            "message": "Script executed successfully." if success else f"Script FAILED with returncode {result.returncode}. Check stderr for details.",
        }

    return execute_python
