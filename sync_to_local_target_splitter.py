
import sys

from casatasks import split, rmtables

track_name = sys.argv[-2]

this_field = sys.argv[-1]

target_name = track_name.split("_")[0]
config = track_name.split("_")[1]
ms_name = track_name.split("_")[2].replace(".tar", "")

# ms_name = sys.argv[-2]


out_ms_name = f"{this_field}_{ms_name}"

rmtables(out_ms_name)

split(vis=ms_name,
      outputvis=out_ms_name,
      field=this_field,
      datacolumn='DATA')
