
'''
Create example job submission scripts.

This isn't generalized to run automatically.
'''

def test_split_template():

    from job_templates import job_import_and_merge

    print(job_import_and_merge.cedar_submission_script(),
          file=open("test_job_split.sh", 'a'))


def test_default_continuum_pipeline_template():

    from job_templates import job_continuum_pipeline

    print(job_continuum_pipeline.cedar_submission_script_default(),
          file=open("test_job_continuum_default.sh", 'a'))

