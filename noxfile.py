import glob

import nox

nox.options.default_venv_backend = "uv|virtualenv"
nox.options.reuse_existing_virtualenvs = True

project = nox.project.load_toml()
python_versions = nox.project.python_versions(project)
locations = "src", "tests"


@nox.session
def build(session: nox.Session) -> None:
    session.install("twine", "uv")
    session.run("uv", "build")
    dists = glob.glob("dist/*")
    session.run("twine", "check", *dists, silent=True)


@nox.session(python=python_versions)
def tests(session: nox.Session) -> None:
    session.install(".[tests]")
    env = {"COVERAGE_FILE": f".coverage.{session.python}"}
    if session.python in ("3.12", "3.13"):
        env["COVERAGE_CORE"] = "sysmon"
    session.run(
        "pytest",
        "--cov",
        "--cov-config=pyproject.toml",
        "--cov-report=xml",
        "--durations=10",
        *session.posargs,
        env=env,
    )


@nox.session
def lint(session: nox.Session) -> None:
    session.install("pre-commit")
    session.install("-e", ".[dev]")

    args = *(session.posargs or ("--show-diff-on-failure",)), "--all-files"
    session.run("pre-commit", "run", *args)
