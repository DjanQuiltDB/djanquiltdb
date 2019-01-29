"""
Python script used by tox to print the code coverage total results to the build log, which will be used by TeamCity to
calculate and display some metrics.
"""
import json
import os
from functools import reduce

status_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'coverage', 'status.json')

# Read and sum the code coverage results from the HTML's status.json
_, statements, missing, _, _, partial, _ = list(
    reduce(
        lambda x, y: [j + y[i] for i, j in enumerate(x)],
        [z['index']['nums'] for z in json.load(open(status_file))['files'].values()]
    )
)

absolute_total = statements
absolute_total_covered = statements - missing - partial

print('##teamcity[buildStatisticValue key=\'CodeCoverageAbsLTotal\' value=\'{}\']'.format(absolute_total))
print('##teamcity[buildStatisticValue key=\'CodeCoverageAbsLCovered\' value=\'{}\']'.format(absolute_total_covered))
