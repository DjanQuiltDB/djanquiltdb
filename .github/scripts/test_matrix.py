"""
Prints the matrix of the test job of the CI workflow as JSON: one entry for each tox environment with the label test.

An entry has the name of the environment, the Python and PostgreSQL versions that its name gives, and the coverage
environment of its suite, when tox has one. The environment of each suite that CODECOV_COMBINATION names also uploads
its coverage to Codecov.
"""

import json
import re
import subprocess
import sys
from pathlib import Path

#: The checkout that this script is in, whose tox configuration is read.
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]

#: The combination whose environment uploads the coverage of its suite, so that each suite has one report.
CODECOV_COMBINATION = 'py315-dj61-pg18'

TEST_ENVIRONMENT_NAME = re.compile(r'^(?P<suite>.+)-(?P<combination>py3(?P<python>\d+)-dj\d+-pg(?P<postgres>\d+))$')


def tox_environment_names(*arguments):
    output = subprocess.run(
        [sys.executable, '-m', 'tox', 'list', '--no-desc', *arguments],
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return [line.strip() for line in output.splitlines() if line.strip()]


def matrix_entry(match, all_environment_names):
    suite = match['suite']
    entry = {'toxenv': match.string, 'python': '3.{}'.format(match['python']), 'postgres': match['postgres']}

    coverage_environment_name = '{}-coverage'.format(suite)
    if coverage_environment_name in all_environment_names:
        entry['coverageenv'] = coverage_environment_name

        if match['combination'] == CODECOV_COMBINATION:
            package_directory = 'djanquiltdb' if suite == 'core' else 'djanquiltdb-plugin-{}'.format(suite)
            entry['codecovflag'] = suite
            entry['coveragexml'] = 'packages/{}/tests/coverage.xml'.format(package_directory)

    return entry


def main():
    all_environment_names = set(tox_environment_names())
    entries = []

    for name in tox_environment_names('-m', 'test'):
        match = TEST_ENVIRONMENT_NAME.match(name)
        if match is None:
            raise ValueError('The test environment {} has no Python, Django and PostgreSQL factors.'.format(name))
        entries.append(matrix_entry(match, all_environment_names))

    print(json.dumps(entries))


if __name__ == '__main__':
    main()
