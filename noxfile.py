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
