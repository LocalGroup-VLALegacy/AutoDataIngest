function addtoQASignUp() {


  var ss = SpreadsheetApp.getActiveSpreadsheet();

  var sheet_newdata = ss.getSheetByName("20A - OpLog Summary");
  var sheet_archivedata = ss.getSheetByName("Archival Track Summary");

  var sheet_signup = ss.getSheetByName("QA Tracks Sign-up");

  var ebid_in_signupsheet = sheet_signup.getRange('C2:C').getDisplayValues().toString().split(",");

  // Loop through the new data and add a new line if the EBID is not found
  var ebids_newdata = sheet_newdata.getRange('H2:H').getDisplayValues().toString().split(",");

  for (var i = 0; i < ebids_newdata.length; i++) {

    this_ebid = ebids_newdata[i]

    var ebid_match = ebid_in_signupsheet.includes(this_ebid)

    var this_status_cont = sheet_newdata.getRange(i+2,1).getValue()
    var this_status_lines = sheet_newdata.getRange(i+2,2).getValue()

    // Skip adding until a status is available. Otherwise, some info will be missing until the pipeline starts

    if (this_status_cont.length > 0) {

      // If not in the list, append a new row:
      if (!ebid_match) {

        this_date = sheet_newdata.getRange(i+2,3).getValue()

        this_trackname = sheet_newdata.getRange(i+2,4).getValue()

        this_target = sheet_newdata.getRange(i+2,5).getValue()

        this_config = sheet_newdata.getRange(i+2,10).getValue()

        sheet_signup.appendRow([this_date, this_trackname, this_ebid, this_target, this_config, this_status_cont, this_status_lines]);

      } else {

        // Find the right row number and update the status

        for (var j = 0; j < ebid_in_signupsheet.length; j++) {

          var this_ebid_match = ebid_in_signupsheet[j].includes(this_ebid)

          if (this_ebid_match) {

            var this_target = sheet_newdata.getRange(i+2,5).getValue()
            var update_target = sheet_signup.getRange(j+2,4).setValue(this_target)

            var update_status = sheet_signup.getRange(j+2,6).setValue(this_status_cont)
            var update_status = sheet_signup.getRange(j+2,7).setValue(this_status_lines)


          }
        }
      }

    }

  }


  var ebids_archivedata = sheet_archivedata.getRange('H2:H').getDisplayValues().toString().split(",");

  for (var i = 0; i < ebids_archivedata.length; i++) {

    this_ebid = ebids_archivedata[i]

    var ebid_match = ebid_in_signupsheet.includes(this_ebid)

    var this_status_cont = sheet_archivedata.getRange(i+2,1).getValue()
    var this_status_lines = sheet_archivedata.getRange(i+2,2).getValue()

    // Skip adding until a status is available. Otherwise, some info will be missing until the pipeline starts

    if (this_status_cont.length > 0) {

      // If not in the list, append a new row:
      if (!ebid_match) {

        this_date = sheet_archivedata.getRange(i+2,3).getValue()

        this_trackname = sheet_archivedata.getRange(i+2,4).getValue()

        this_target = sheet_archivedata.getRange(i+2,5).getValue()

        this_config = sheet_archivedata.getRange(i+2,10).getValue()

        sheet_signup.appendRow([this_date, this_trackname, this_ebid, this_target, this_config, this_status_cont, this_status_lines]);

      } else {

        // Find the right row number and update the status

        for (var j = 0; j < ebid_in_signupsheet.length; j++) {

          var this_ebid_match = ebid_in_signupsheet[j].includes(this_ebid)

          if (this_ebid_match) {

            var this_target = sheet_archivedata.getRange(i+2,5).getValue()
            var update_target = sheet_signup.getRange(j+2,4).setValue(this_target)

            var update_status = sheet_signup.getRange(j+2,6).setValue(this_status_cont)
            var update_status = sheet_signup.getRange(j+2,7).setValue(this_status_lines)

          }
        }
      }

    }

  }

}
