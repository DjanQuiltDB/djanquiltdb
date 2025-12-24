"""
Python script used by tox to print the code coverage total results to the build log.
"""

import json
import os

status_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'coverage', 'status.json')

# Read and sum the code coverage results from the HTML's status.json
if not os.path.exists(status_file):
    print('Coverage report not found.')
    exit(0)

try:
    with open(status_file, 'r') as f:
        coverage_data = json.load(f)

    files_data = coverage_data.get('files', {})
    if not files_data:
        print('No coverage data found.')
        exit(0)

    # Sum up all the coverage numbers
    # Format: [n_files, n_lines, n_statements, n_missing, n_branches, n_partial, n_missing_branches, n_excluded]
    totals = [0] * 8

    for file_info in files_data.values():
        nums = file_info.get('index', {}).get('nums', [])
        if not nums:
            continue

        # Handle both list and dict formats
        if isinstance(nums, dict):
            # If nums is a dict, convert to list format
            nums_list = [
                nums.get('n_files', 0),
                nums.get('n_lines', 0),
                nums.get('n_statements', 0),
                nums.get('n_missing', 0),
                nums.get('n_branches', 0),
                nums.get('n_partial', 0),
                nums.get('n_missing_branches', 0),
                nums.get('n_excluded', 0),
            ]
        elif isinstance(nums, list) and len(nums) >= 8:
            nums_list = nums
        else:
            continue

        for i in range(8):
            totals[i] += nums_list[i]

    statements = totals[2]
    missing = totals[3]
    partial = totals[5]

    absolute_total = statements
    absolute_total_covered = statements - missing - partial
    coverage_percent = (absolute_total_covered / absolute_total * 100) if absolute_total > 0 else 0

    print('Coverage Summary:')
    print(f'  Total lines: {absolute_total}')
    print(f'  Covered lines: {absolute_total_covered}')
    print(f'  Coverage: {coverage_percent:.2f}%')
except Exception as e:
    print(f'Error processing coverage data: {e}')
    import traceback

    traceback.print_exc()
    exit(1)
