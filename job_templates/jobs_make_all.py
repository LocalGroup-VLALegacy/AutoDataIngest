

def make_all_pipeline_scripts(machine_name='cedar'):
    '''
    Make all of the pipeline job scripts given the specified machine they are to run on.
    '''

    if machine_name == "cedar":

        from job_import_and_merge import cedar_submission_script as make_import_and_merge_script
        from job_line_pipeline import cedar_submission_script_default as make_line_pipeline_script
        from job_continuum_pipeline.py import cedar_submission_script_default as make_continuum_pipeline_script

    else:
        raise ValueError("Scripts only currently produced for the cedar cluster.")



