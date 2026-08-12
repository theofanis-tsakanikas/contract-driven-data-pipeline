"""Guards on the pins that must agree across files, and on the Airflow version.

These invariants used to live only in comments — ``requirements-spark.txt`` says
"pinned to match the Airflow image versions!" and nothing enforced it — so an
automated dependency bump could raise the version in one file, leave the other
behind, and collect five green checks.

The specific failures these tests exist to catch, all observed on real Dependabot
pull requests against this repository:

* ``apache/airflow`` bumped from ``2.11.0`` to ``3.3.0`` in ``Dockerfile.airflow``
  while CI kept installing ``apache-airflow==2.11.0`` — so the DAG tests validated
  a version the image no longer shipped, and the PR was green.
* ``pandas`` bumped in one requirements file and not the other, silently breaking
  the "same version in the Airflow and Spark images" rule the pipeline relies on
  when a DataFrame crosses between them.

Pure stdlib: no Spark, no network, milliseconds.
"""
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]

AIRFLOW_REQS = REPO / "infra" / "requirements-airflow.txt"
SPARK_REQS = REPO / "infra" / "requirements-spark.txt"
DEV_REQS = REPO / "requirements-dev.txt"
DOCKERFILE_AIRFLOW = REPO / "infra" / "Dockerfile.airflow"
CI_WORKFLOW = REPO / ".github" / "workflows" / "ci.yml"

# Packages that must carry the identical pin in both images: a DataFrame written by
# the Spark image is read by the Airflow image, so a mismatch is a serialisation bug
# waiting for the worst possible moment.
SHARED_PINS = ["pandas", "boto3", "botocore", "psycopg2-binary", "pyspark", "pyarrow"]


def _pins(path: Path) -> dict[str, str]:
    """Parse `name==version` lines. Ignores comments, blanks, `-r` and unpinned names."""
    pins = {}
    for raw in path.read_text().splitlines():
        line = raw.split("#")[0].strip()
        if not line or line.startswith("-"):
            continue
        match = re.fullmatch(r"([A-Za-z0-9_.\-]+)==([^\s;]+)", line)
        if match:
            pins[match.group(1).lower()] = match.group(2)
    return pins


@pytest.mark.parametrize("package", SHARED_PINS)
def test_shared_pins_agree_across_both_images(package):
    """A package pinned in both requirement files must carry the same version."""
    airflow, spark = _pins(AIRFLOW_REQS), _pins(SPARK_REQS)
    if package not in airflow or package not in spark:
        pytest.skip(f"{package} is not pinned in both files")
    assert airflow[package] == spark[package], (
        f"{package} is {airflow[package]} in requirements-airflow.txt but "
        f"{spark[package]} in requirements-spark.txt. The two images must ship the "
        f"same version — bump both, in the same commit, or neither."
    )


def test_faker_pin_matches_the_airflow_image():
    """requirements-dev.txt re-pins Faker for the tests; it must not drift."""
    assert _pins(DEV_REQS)["faker"] == _pins(AIRFLOW_REQS)["faker"], (
        "requirements-dev.txt and infra/requirements-airflow.txt disagree on the "
        "Faker version, so the generator's tests would exercise a version the "
        "pipeline does not ship."
    )


def _airflow_version_from_dockerfile() -> str:
    match = re.search(
        r"^FROM\s+apache/airflow:(\d+\.\d+\.\d+)-python", DOCKERFILE_AIRFLOW.read_text(), re.M
    )
    assert match, "Could not read the Airflow version from the Dockerfile's FROM line"
    return match.group(1)


def test_ci_validates_the_airflow_version_the_image_actually_ships():
    """The DAG tests must run against the version in the Dockerfile, not beside it.

    Three places state the Airflow version — the base image, the pinned install in
    the dag-validate job, and the constraints URL. All three must agree, or CI is
    validating a version nobody runs.
    """
    image_version = _airflow_version_from_dockerfile()
    ci = CI_WORKFLOW.read_text()

    installed = re.search(r'"apache-airflow==(\d+\.\d+\.\d+)"', ci)
    assert installed, "ci.yml no longer pins apache-airflow in the dag-validate job"
    assert installed.group(1) == image_version, (
        f"Dockerfile.airflow ships Airflow {image_version} but ci.yml validates the "
        f"DAG against {installed.group(1)}. A base-image bump that leaves CI behind "
        f"passes every check while breaking the stack."
    )

    constraints = re.search(r"constraints-(\d+\.\d+\.\d+)/constraints-", ci)
    assert constraints, "ci.yml no longer references an Airflow constraints file"
    assert constraints.group(1) == image_version, (
        f"The constraints file targets Airflow {constraints.group(1)}, but the image "
        f"ships {image_version}."
    )


def test_python_minor_agrees_between_the_image_and_ci():
    """The base image's Python and the constraints file's Python must match."""
    image = re.search(r"^FROM\s+apache/airflow:[\d.]+-python(\d+\.\d+)", DOCKERFILE_AIRFLOW.read_text(), re.M)
    assert image, "Could not read the Python version from the Dockerfile's FROM line"
    ci = re.search(r"constraints-[\d.]+/constraints-(\d+\.\d+)\.txt", CI_WORKFLOW.read_text())
    assert ci, "ci.yml no longer references a versioned constraints file"
    assert image.group(1) == ci.group(1), (
        f"The Airflow image is built on Python {image.group(1)} but CI resolves "
        f"against the Python {ci.group(1)} constraints."
    )
