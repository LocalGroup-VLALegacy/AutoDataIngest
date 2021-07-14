function addtoQASignUp() {


  var ss = SpreadsheetApp.getActiveSpreadsheet();

  var sheet_newdata = ss.getSheetByName("20A - OpLog Summary");
  var sheet_archivedata = ss.getSheetByName("Archival Track Summary");

  var sheet_signup = ss.getSheetByName("QA Tracks Sign-up");

  var ebid_in_signupsheet = sheet_signup.getRange('C2:C').getDisplayValues().toString().split(",");

  // Get the current number of rows
  var num_rows = getFirstEmptyRowByColumnArray()
  Logger.log(num_rows)

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

        var update_vals = sheet_signup.getRange(num_rows + 1, 1).setValue(this_date)
        var update_vals = sheet_signup.getRange(num_rows + 1, 2).setValue(this_trackname)
        var update_vals = sheet_signup.getRange(num_rows + 1, 3).setValue(this_ebid)
        var update_vals = sheet_signup.getRange(num_rows + 1, 4).setValue(this_target)
        var update_vals = sheet_signup.getRange(num_rows + 1, 5).setValue(this_config)
        var update_vals = sheet_signup.getRange(num_rows + 1, 6).setValue(this_status_cont)
        var update_vals = sheet_signup.getRange(num_rows + 1, 7).setValue(this_status_lines)

        var num_rows = num_rows + 1

        // sheet_signup.appendRow([this_date, this_trackname, this_ebid, this_target, this_config, this_status_cont, this_status_lines]);

      } else {

        // Find the right row number and update the status

        for (var j = 0; j < ebid_in_signupsheet.length; j++) {

          var this_ebid_match = ebid_in_signupsheet[j].includes(this_ebid)

          if (this_ebid_match) {

            var this_target = sheet_newdata.getRange(i+2,5).getValue()
            var update_target = sheet_signup.getRange(j+2,4).setValue(this_target)

            var this_trackname = sheet_newdata.getRange(i+2,4).getValue()
            var update_trackname = sheet_signup.getRange(j+2,2).setValue(this_trackname)

            var old_status_cont = sheet_signup.getRange(j+2,6).getValue()
            var old_status_lines = sheet_signup.getRange(j+2,7).getValue()

            var update_status = sheet_signup.getRange(j+2,6).setValue(this_status_cont)
            var bkg_colour_cont = sheet_newdata.getRange(i+2,1).getBackground()
            var update_status = sheet_signup.getRange(j+2,6).setBackground(bkg_colour_cont)
            var font_colour_cont = sheet_newdata.getRange(i+2,1).getFontColor()
            var update_status = sheet_signup.getRange(j+2,6).setFontColor(font_colour_cont)


            var update_status = sheet_signup.getRange(j+2,7).setValue(this_status_lines)
            var bkg_colour_lines = sheet_newdata.getRange(i+2,2).getBackground()
            var update_status = sheet_signup.getRange(j+2,7).setBackground(bkg_colour_lines)
            var font_colour_lines = sheet_newdata.getRange(i+2,2).getFontColor()
            var update_status = sheet_signup.getRange(j+2,7).setFontColor(font_colour_lines)

            // Did the status change to "ready for QA"? If so, ping the reviewer
            var exp_qa_status = "Ready for QA"
            var in_previously_qa_cont = old_status_cont.includes(exp_qa_status)
            var in_previously_qa_lines = old_status_lines.includes(exp_qa_status)

            var new_qa_cont = this_status_cont.includes(exp_qa_status)
            var new_qa_lines = this_status_lines.includes(exp_qa_status)


            var do_send_email = false
            var update_string = ""
            // Check if things have changes:
            if (!in_previously_qa_cont && new_qa_cont){
              do_send_email = true
              update_string = update_string + 'Continuum'
            }

            if (!in_previously_qa_lines && new_qa_lines){
              do_send_email = true
              var update_string
              if (update_string.length > 0) {
                update_string = update_string + '/Speclines'
              } else {
                update_string = 'Speclines'
              }
            }

            if (do_send_email) {
              // Record which continuum/lines is ready for review
              sheet_signup.getRange(j+2,16).setValue(update_string)

              var emailAddress = sheet_signup.getRange(j+2,9).getValue()
              var subject = sheet_signup.getRange(j+2,12).getValue()
              var message = sheet_signup.getRange(j+2,13).getValue()

              if (emailAddress.length > 0) {
                MailApp.sendEmail(emailAddress, subject, message)
              }
            }

            // Set to "ready for imaging" color to the whole row when a track is "finished"
            var finish_qa_status = "Ready for imaging"
            if ((this_status_cont == finish_qa_status) && (this_status_lines == finish_qa_status)) {
              var update_row_colour = sheet_signup.getRange(j+2, 1, 1, 10).setBackground(bkg_colour_lines)
              var update_row_colour = sheet_signup.getRange(j+2, 1, 1, 10).setFontColor(font_colour_lines)
            }

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

      Logger.log(this_ebid)
      Logger.log(ebid_match)
      Logger.log(this_status_cont)
      Logger.log(this_status_lines)

      // If not in the list, append a new row:
      if (!ebid_match) {

        Logger.log("Found new track")

        this_date = sheet_archivedata.getRange(i+2,3).getValue()

        this_trackname = sheet_archivedata.getRange(i+2,4).getValue()

        this_target = sheet_archivedata.getRange(i+2,5).getValue()

        this_config = sheet_archivedata.getRange(i+2,10).getValue()

        var update_vals = sheet_signup.getRange(num_rows + 1, 1).setValue(this_date)
        var update_vals = sheet_signup.getRange(num_rows + 1, 2).setValue(this_trackname)
        var update_vals = sheet_signup.getRange(num_rows + 1, 3).setValue(this_ebid)
        var update_vals = sheet_signup.getRange(num_rows + 1, 4).setValue(this_target)
        var update_vals = sheet_signup.getRange(num_rows + 1, 5).setValue(this_config)
        var update_vals = sheet_signup.getRange(num_rows + 1, 6).setValue(this_status_cont)
        var update_vals = sheet_signup.getRange(num_rows + 1, 7).setValue(this_status_lines)

        var num_rows = num_rows + 1

        // sheet_signup.appendRow([this_date, this_trackname, this_ebid, this_target, this_config, this_status_cont, this_status_lines]);

      } else {

        // Find the right row number and update the status

        for (var j = 0; j < ebid_in_signupsheet.length; j++) {

          var this_ebid_match = ebid_in_signupsheet[j].includes(this_ebid)

          if (this_ebid_match) {

            var this_target = sheet_archivedata.getRange(i+2,5).getValue()
            var update_target = sheet_signup.getRange(j+2,4).setValue(this_target)

            var this_trackname = sheet_archivedata.getRange(i+2,4).getValue()
            var update_trackname = sheet_signup.getRange(j+2,2).setValue(this_trackname)

            var old_status_cont = sheet_signup.getRange(j+2,6).getValue()
            var old_status_lines = sheet_signup.getRange(j+2,7).getValue()

            var update_status = sheet_signup.getRange(j+2,6).setValue(this_status_cont)
            var bkg_colour_cont = sheet_archivedata.getRange(i+2,1).getBackground()
            var update_status = sheet_signup.getRange(j+2,6).setBackground(bkg_colour_cont)
            var font_colour_cont = sheet_archivedata.getRange(i+2,1).getFontColor()
            var update_status = sheet_signup.getRange(j+2,6).setFontColor(font_colour_cont)


            var update_status = sheet_signup.getRange(j+2,7).setValue(this_status_lines)
            var bkg_colour_lines = sheet_archivedata.getRange(i+2,2).getBackground()
            var update_status = sheet_signup.getRange(j+2,7).setBackground(bkg_colour_lines)
            var font_colour_lines = sheet_archivedata.getRange(i+2,2).getFontColor()
            var update_status = sheet_signup.getRange(j+2,7).setFontColor(font_colour_lines)

            // Did the status change to "ready for QA"? If so, ping the reviewer
            var exp_qa_status = "Ready for QA"
            var in_previously_qa_cont = old_status_cont.includes(exp_qa_status)
            var in_previously_qa_lines = old_status_lines.includes(exp_qa_status)

            var new_qa_cont = this_status_cont.includes(exp_qa_status)
            var new_qa_lines = this_status_lines.includes(exp_qa_status)


            var do_send_email = false
            var update_string = ""
            // Check if things have changes:
            if (!in_previously_qa_cont && new_qa_cont){
              do_send_email = true
              update_string = update_string + 'Continuum'
            }

            if (!in_previously_qa_lines && new_qa_lines){
              do_send_email = true
              var update_string
              if (update_string.length > 0) {
                update_string = update_string + '/Speclines'
              } else {
                update_string = 'Speclines'
              }
            }

            if (do_send_email) {
              // Record which continuum/lines is ready for review
              sheet_signup.getRange(j+2,16).setValue(update_string)

              var emailAddress = sheet_signup.getRange(j+2,9).getValue()
              var subject = sheet_signup.getRange(j+2,12).getValue()
              var message = sheet_signup.getRange(j+2,13).getValue()

              if (emailAddress.length > 0) {
                MailApp.sendEmail(emailAddress, subject, message)
              }

            }

            // Set to "ready for imaging" color to the whole row when a track is "finished"
            var finish_qa_status = "Ready for imaging"
            if ((this_status_cont == finish_qa_status) && (this_status_lines == finish_qa_status)) {
              var update_row_colour = sheet_signup.getRange(j+2, 1, 1, 10).setBackground(bkg_colour_lines)
              var update_row_colour = sheet_signup.getRange(j+2, 1, 1, 10).setFontColor(font_colour_lines)
            }


          }
        }
      }

    }

  }

}

function getFirstEmptyRowByColumnArray() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet_signup = ss.getSheetByName("QA Tracks Sign-up");
  var ebids_signup = sheet_signup.getRange('C2:C')
  //.getDisplayValues().toString().split(",");
  var values = ebids_signup.getValues(); // get all data in one call
  var ct = 0;
  while ( values[ct] && values[ct][0] != "" ) {
    ct++;
  }
  return (ct+1);
}