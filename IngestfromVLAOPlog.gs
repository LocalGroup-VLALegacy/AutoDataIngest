// From https://stackoverflow.com/questions/31345400/extract-info-from-email-body-with-google-scripts
// Adapted from https://gist.github.com/Ferrari/9678772
// Updates for project by E. Koch. Pass issues via LG VLAXL slack or at koch.eric.w@gmail.com

function IngestOpLogToSheet() {

  // Have to get data separate to avoid google app script limit!

  var start = 0;
  //  var label = GmailApp.getUserLabelByName("Telescope Notifications");
  var label = GmailApp.getUserLabelByName("Telescope Notifications/20A-346");
  var threads = label.getThreads();

  var ss = SpreadsheetApp.getActiveSpreadsheet();
  //  var sheet = ss.getActiveSheet();


  // This should be split to ensure the correct sheet is used.
  // OR we make a new sheet when the semesters change. Might be a manual change
  // ~6 times.
  var sheet = ss.getSheetByName("20A - OpLog Summary");

  // Test against existing 19B-037
//  var sheet = ss.getSheetByName("SCRIPT_TEST");

  // Grab all of the log names already in the sheet. Will skip if already recorded.
  var lognames = sheet.getRange('E2:E').getDisplayValues().toString().split(",");

  //  Logger.log(lognames)

  for (var i = 0; i < threads.length; i++) {
    var messages = threads[i].getMessages();

    var content = messages[0].getPlainBody();

    var subject = messages[0].getSubject();

    var subj_match
    subj_match = subject.includes("VLA operator")

    var projcode = "20A-346"

    // Test case on old project
    //    var projcode = "19B-037"

    var proj_match = subject.includes(projcode)

    if (subj_match && proj_match) {

      if (content) {

        // If log name not in spreadsheet, continue
        var tmp;
        // e.g. 2020-01-27_1315_19B-037
        // \d{4}-\d{2}-d{2}_
        tmp = content.match(/(?:Log Name)\W+(\d{4}-\d{2}-\d{2}_\d{4}_\S{7})/);
        var logname = (tmp && tmp[1]) ? tmp[1] : 'No logname';

        var log_match = lognames.includes(logname);

        if (!log_match) {

          // Date
          tmp = content.match(/(?:Observing Date)\W+(\d{2}-\S{3}-\d{4})/);
          var obsdate = (tmp && tmp[1]) ? tmp[1] : 'No date';

          Logger.log(obsdate)

          tmp = content.match(/(?:SBID)\S{3}\W+(\d{8})/);
          var sbblock = (tmp && tmp[1]) ? tmp[1] : 'No SBID';

          tmp = content.match(/(?:EBID)\S{3}\W+(\d{8})/);
          var ebblock = (tmp && tmp[1]) ? tmp[1] : 'No EBID';

          tmp = content.match(/(?:Source File)\S{3}\W+(\S{7}_sb\d{8}_\S+_\S+)/);
          var source_file = (tmp && tmp[1]) ? tmp[1].trim() : 'No email';

          tmp = content.match(/(?:Array Configuration:)\W+(\D{1})/);
          var arrconfig = (tmp && tmp[1]) ? tmp[1] : 'No config';

          //tmp = content.match(/(?:\d{2}-\S{3}-\d{4})\W+(\d{2}:\d{2}:\d{2})/);
          //var starttime = (tmp && tmp[1]) ? tmp[1] : 'No start time';
          //Logger.log(tmp)

          tmp = content.match(/(?:\d{2}\S{3})\W+(\d{2}:\d{2}:\d{2})/);
          var endtime = (tmp && tmp[1]) ? tmp[1] : 'No end time';

          // There's definitely better ways to do this... but this does work
          tmp = content.match(/(?:\d{2}-\S{3}-\d{4})\W+(?:\d{2}:\d{2}:\d{2})\W+([\d\.\d]+)/);
          var totaltime = (tmp && tmp[1]) ? tmp[1] : 'No total time';

          tmp = content.match(/(?:\d{2}-\S{3}-\d{4})\W+(?:\d{2}:\d{2}:\d{2})\W+(?:[\d\.\d]+)\W+([\d\.\d]+)/);
          var percdown = (tmp && tmp[1]) ? tmp[1] : 'No perc down time';

          tmp = content.match(/(?:\d{2}-\S{3}-\d{4})\W+(?:\d{2}:\d{2}:\d{2})\W+(?:[\d\.\d]+)\W+(?:[\d\.\d]+%)\W+([\d\.\d]+)/);
          var totaldowntime = (tmp && tmp[1]) ? tmp[1] : 'No perc down time';

//          Logger.log(tmp)

          // Order into spreadsheet is:
          // Date	Log name	"Sched. Block ID (SBID)"
          // "Exec. Block ID (EBID)"	Source File	Configuration
          // End Time	Total Time	% Down Time	Total Down Time
          sheet.appendRow([null, obsdate, null, null, logname, sbblock, ebblock, source_file, arrconfig,
                           endtime, totaltime, percdown, totaldowntime]);


        }
      }

//      Utilities.sleep(600);
    }
  }
};
