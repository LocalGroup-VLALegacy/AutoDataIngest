function CheckFlaggingforRestart() {
  // Transfer input from the QA reviewer into the main spreadsheet.
  // This enables triggering re-runs of the pipeline and completion of the QA process.

  var ss = SpreadsheetApp.getActiveSpreadsheet();

  // This should be split to ensure the correct sheet is used.
  var sheet = ss.getSheetByName("20A - OpLog Summary");

  var ss_flag = SpreadsheetApp.openById('1ROuailtqnaCJVI_ek_perxN8ki9siSUrV_N-bLC3Dqg')

  var ss_flag_all_sheets = ss_flag.getSheets()

  var ebids = sheet.getRange('G2:G').getDisplayValues().toString().split(",");


  for (var i = 0; i < ss_flag_all_sheets.length; i++) {
    var sheetname = ss_flag_all_sheets[i].getName();

    // Logger.log(sheetname)

    for (var j = 0; j < ebids.length; j++) {

      if (ebids[j].length > 0) {
        // Logger.log(ebids[j])

        var ebid_match = sheetname.includes(ebids[j])

        if (ebid_match) {

          Logger.log(sheetname)
          Logger.log(ebids[j])

          var this_ebid = ebids[j]

          var restart_type = ss_flag_all_sheets[i].getRange('M1').getValue()

          var refant_avoid = ss_flag_all_sheets[i].getRange('O1').getValue()

          // Pass along any antennas noted to avoid as the refant
          if (refant_avoid.length > 0) {
            thiscell = sheet.getRange(j+2, 30)
            thiscell.setValue(refant_avoid)
          }

          // Restart the line components
          if (sheetname.includes('speclines')) {

            thiscell = sheet.getRange(j+2, 29)

            if (thiscell.length > 0) {
              thiscell.setValue(restart_type)

              ss_flag_all_sheets[i].getRange('M1').setValue('')

              ss_flag_all_sheets[i].getRange('M2').setValue('Status imported into sheet tracker.')

            }

          }

          // Restart the continuum component
          if (sheetname.includes('continuum')) {

            thiscell = sheet.getRange(j+2, 28)

            if (thiscell.length > 0) {
              thiscell.setValue(restart_type)

              Logger.log(thiscell.length)

              ss_flag_all_sheets[i].getRange('M1').setValue('')

              ss_flag_all_sheets[i].getRange('M2').setValue('Status imported into sheet tracker.')

            }

          }

        }

      }

    }

  }

}
