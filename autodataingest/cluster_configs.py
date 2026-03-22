
'''
These are dictionaries containing the info required about the clusters
that are used to do the reduction and to produce job scripts.
'''

import autodataingest.job_templates.job_unified_pipeline as jobs_unified


# Add new locations here so we can refer to each location by 1 name:
ENDPOINT_INFO = {'cc-cedar': {'endpoint_id': "8dec4129-9ab4-451d-a45f-5b4b8471f7a3",
                           'data_path': "scratch/VLAXL_reduction/",
                           'data_path_staging': "/home/ekoch/projects/rrg-eros-ab/ekoch/VLAXL/sdm_staging/"},
                'dra-fir': {'endpoint_id': "8dec4129-9ab4-451d-a45f-5b4b8471f7a3",
                           'data_path': "scratch/VLAXL_reduction/",
                           'data_path_staging': "/home/ekoch/projects/rrg-eros-ab/ekoch/VLAXL/sdm_staging/"},
                 'nrao-aoc': {'endpoint_id': "62708910-8e89-11e8-9641-0a6d4e044368",
                              'data_path': '/lustre/aoc/projects/20A-346/data_staged_new/'},
                 'msu-hpcc': {'endpoint_id': "a640bafc-6d04-11e5-ba46-22000b92c6ec",
                             'data_path': "/mnt/research/ChomiukLab/LocalGroupX/M31_20A-346/"},
                 'ingester': {'endpoint_id': "e8fc98cc-9ca8-11eb-92cd-6b08dd67ff48",
                              'data_path': "/mnt/space/vlaxl/track_products/",
                              'qa_path': "/mnt/bigdata/vlaxl/data_staging/"}}

JOB_CREATION_FUNCTIONS = \
    {'cc-cedar': {'UNIFIED_PIPE': jobs_unified.cedar_submission_script},
     'dra-fir':  {'UNIFIED_PIPE': jobs_unified.cedar_submission_script},
    }

# Note that these are now broken down by job execution type
# These are defined in the ssh config file.
# CLUSTERADDRS = \
#     {'cc-cedar': 'robot.cedar.alliancecan.ca'}

# Re-use cedar scripts for fir's robot submission.
CLUSTERADDRS = \
    {'cedar-submitter': 'cedar-submitter',
     'cedar-robot-generic': 'cedar-robot-generic',
     'cedar-robot-jobsetup': 'cedar-robot-jobsetup',
     'cedar-robot-jobstatus': 'cedar-robot-jobstatus',
     'cedar-robot-lfsquota': 'cedar-robot-lfsquota',
     }
