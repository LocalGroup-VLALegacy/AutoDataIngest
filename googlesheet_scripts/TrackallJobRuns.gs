
function TrackJobRuns() {

  // Record all email job notifications. Currently works for the cedar cluster.

  // Have to get data separate to avoid google app script limit!

  var start = 0;
  //  var label = GmailApp.getUserLabelByName("Telescope Notifications");
  var label = GmailApp.getUserLabelByName("Telescope Notifications/20A-346 Cedar Jobs");
  var threads = label.getThreads();

  // Try ordering from oldest to newest
  threads.reverse();

  var ss = SpreadsheetApp.getActiveSpreadsheet();
  //  var sheet = ss.getActiveSheet();


  // This should be split to ensure the correct sheet is used.
  // OR we make a new sheet when the semesters change. Might be a manual change
  // ~6 times.
  var sheet = ss.getSheetByName("Jobs Summary");


  // Grab all of the log names already in the sheet. Will skip if already recorded.
  var existing_jobids = sheet.getRange('D2:D').getDisplayValues().toString().split(",");

  //  Logger.log(lognames)

  for (var i = 0; i < threads.length; i++) {
    var messages = threads[i].getMessages();

    var subject = messages[0].getSubject();

    var run_date = messages[0].getDate()

    var subj_match = subject.includes("Slurm")

    if (subj_match) {

        // If log name not in spreadsheet, continue
        var tmp;
        // e.g. 2020-01-27_1315_19B-037
        // \d{4}-\d{2}-d{2}_

        tmp = subject.match(/Job_id=(.\S+)/);
        var job_id = (tmp && tmp[1]) ? tmp[1] : 'No Job ID';

        var log_match = existing_jobids.includes(job_id);

        if (!log_match) {

          tmp = subject.match(/Name=(.\S+)/);
          var job_name = (tmp && tmp[1]) ? tmp[1] : 'No job name';

          // Get the job type:
          var job_type = null
          if (job_name.includes('line_pipeline')) {
            job_type = 'line pipeline'
          } else if (job_name.includes('continuum_pipeline')) {
            job_type = 'continuum pipeline'
          } else {
            job_type = "split"
          }

          tmp = subject.match(/Run time (.\S+),/)
          var run_time = (tmp && tmp[1]) ? tmp[1] : 'No run time';

          tmp = subject.match(/,(.*$)/)
          var comma_splits = (tmp && tmp[1]) ? tmp[1] : 'No job status';
          job_status = comma_splits.split(",")[1]
          exit_code = comma_splits.split(",")[2]

          Logger.log(run_date)
          // Logger.log(job_name)
          // Logger.log(job_type)
          // Logger.log(job_id)
          // Logger.log(job_status)
          // Logger.log(exit_code)
          // Logger.log(run_time)

          sheet.appendRow([run_date, job_name, job_type, job_id, job_status, exit_code, run_time]);


        }

//      Utilities.sleep(600);
    }
  }
};
