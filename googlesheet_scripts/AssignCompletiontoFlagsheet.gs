function ShowCompletioninFlaggingSheet() {
  // Transfer input from the QA reviewer into the main spreadsheet.
  // This enables triggering re-runs of the pipeline and completion of the QA process.

  var ss = SpreadsheetApp.getActiveSpreadsheet();

  var ss_flag = SpreadsheetApp.openById('1ROuailtqnaCJVI_ek_perxN8ki9siSUrV_N-bLC3Dqg')

  var ss_flag_all_sheets = ss_flag.getSheets()

  // This should be split to ensure the correct sheet is used.
  const sheetnames = ["20A - OpLog Summary", "Archival Track Summary"];

  Logger.log(sheetnames[0])

  for (var jj = 0; jj < sheetnames.length; jj++) {

    var sheetname_summary = sheetnames[jj]

    Logger.log(sheetname_summary)

    var sheet = ss.getSheetByName(sheetname_summary);


    var ebids = sheet.getRange('H2:H').getDisplayValues().toString().split(",");
    var continuum_status = sheet.getRange('A2:A').getDisplayValues().toString().split(",");
    var line_status = sheet.getRange('B2:B').getDisplayValues().toString().split(",");

    for (var i = 0; i < ss_flag_all_sheets.length; i++) {
      var sheetname = ss_flag_all_sheets[i].getName();

      // Logger.log(sheetname)

      for (var j = 0; j < ebids.length; j++) {

        if (ebids[j].length > 0) {
          // Logger.log(ebids[j])

          var ebid_match = sheetname.includes(ebids[j])

          var this_continuum_status = continuum_status[j]
          var this_line_status = line_status[j]

          if (ebid_match) {

            Logger.log(sheetname)
            Logger.log(ebids[j])

            var this_ebid = ebids[j]

            var restart_type = ss_flag_all_sheets[i].getRange('M1').getValue()


            // Restart the line components
            if (sheetname.includes('speclines')) {

              if (this_line_status.includes('Ready for imaging')) {

                ss_flag_all_sheets[i].getRange('O2').setValue('COMPLETED')
                ss_flag_all_sheets[i].getRange('O2').setBackground('lightgreen')
              }

            }

            // Restart the continuum component
            if (sheetname.includes('continuum')) {

              if (this_continuum_status.includes('Ready for imaging')) {

                ss_flag_all_sheets[i].getRange('O2').setValue('COMPLETED')
                ss_flag_all_sheets[i].getRange('O2').setBackground('lightgreen')

              }

            }

          }

        }

      }

    }

  }

}
