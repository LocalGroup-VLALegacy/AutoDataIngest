
'''
These are dictionaries containing the info required about the clusters
that are used to do the reduction and to produce job scripts.
'''

import .job_templates.job_import_and_merge as jobs_import
import .job_templates.job_continuum_pipeline as jobs_continuum
import .job_templates.job_line_pipeline as jobs_line


# Add new locations here so we can refer to each location by 1 name:
ENDPOINT_INFO = {'cc-cedar': {'endpoint_id': "c99fd40c-5545-11e7-beb6-22000b9a448b",
                           'data_path': "scratch/VLAXL_reduction/"},
                 'nrao-aoc': {'endpoint_id': "62708910-8e89-11e8-9641-0a6d4e044368",
                              'data_path': '/lustre/aoc/projects/20A-346/data_staged/'},
                 'msu-hpcc': {'endpoint_id': "a640bafc-6d04-11e5-ba46-22000b92c6ec",
                             'data_path': "/mnt/research/ChomiukLab/LocalGroupX/M31_20A-346/"},
                 'ingester': {'endpoint_id': "ad5427e4-1027-11eb-81b1-0e2f230cc907",
                              'data_path': "/home/ekoch/"}}

JOB_CREATION_FUNCTIONS = \
    {'cc-cedar': {'IMPORT_SPLIT': jobs_import.cedar_submission_script,
                  'CONTINUUM_PIPE': jobs_continuum.cedar_submission_script,
                  'LINE_PIPE': jobs_line.cedar_submission_script}}

CLUSTERADDRS = \
    {'cc-cedar': 'cedar.computecanada.ca'}
