
import sys
import os
from pathlib import Path
import tarfile

from qaplotter import make_all_plots

# Fixed folder where all pipeline products are stored.
products_folder = Path(sys.argv[-1])

finalproducts_folder = products_folder / "withQA_tars"
finalproducts_folder.mkdir(exist_ok=True)

# Raise errors or print warning to terminal
strict_check = False

track_products = products_folder.glob("**/*.tar")

for track_tar in track_products:

    print(f"Extracting {track_tar}")

    # Untar to its own directory.
    folder_name = products_folder / track_tar.name.rstrip(".tar")
    if not os.path.exists(folder_name):
        os.mkdir(folder_name)
    else:
        # Skip existing.
        print("Output exists. Skipping.")
        continue

    with tarfile.open(track_tar) as tf:

        tf.extractall(path=folder_name)

    # Identify the folder we need for QA:
    # 1. weblog
    # 2. scan_plots_txt (field QA output for interactive plots)
    # 3. finalBPcal_txt (bandpass QA output for interactive plots)
    # 4. uvresid_plots (just need to embed in an HTML file)

    track_extracted = folder_name / "products"
    qa_output = folder_name

    print(f"Track extracted folder: {track_extracted}")

    has_scanplotstxt = False
    has_BPcaltxt = False
    has_uvresidplots = False

    for prod in track_extracted.iterdir():

        if "weblog" in str(prod):

            print("Found weblog")

            # Move to qa_output, then extract.
            output_prod = qa_output / prod.name

            # really shouldn't exist already...
            if not output_prod.exists():
                prod.replace(output_prod)
            else:
                ValueError("The weblog already exists in the parent folder. This is a bug.")

            # Extract the tgz file to the QA output folder
            with tarfile.open(output_prod) as tf:

                tf.extractall(path=qa_output)

            # Delete the copied weblog.tgz
            output_prod.unlink()

            print(f"Extracted weblog to {qa_output / output_prod.name}")

            continue

        if "scan_plots_txt" in str(prod):
            has_scanplotstxt = True
            scan_plots_txt_path = prod
            continue

        if "finalBPcal_txt" in str(prod):
            has_BPcaltxt = True
            finalBPcal_txt_path = prod

            continue

        if "uvresid_plots" in str(prod):
            has_uvresidplots = True
            uvresidplots_path = prod

    # Check we have the text files to make the QA plots before making them

    if not has_scanplotstxt or not has_BPcaltxt or not has_uvresidplots:
        if strict_check:
            raise ValueError("Cannot find `scan_plots_txt` or `finalBPcal_txt` in the products folder."
                             " Unable to generate QA plots.")
        else:
            print("Cannot find `scan_plots_txt` or `finalBPcal_txt` in the products folder."
                  " Unable to generate complete QA plots.")

    print("Making QA plots")

    make_all_plots(folder_fields=scan_plots_txt_path,
                   output_folder_fields=qa_output / "scan_plots_QAplots",
                   folder_BPs=finalBPcal_txt_path,
                   output_folder_BPs=qa_output / "finalBPcal_QAplots")
    # Add in passing uvresid plot info here.

    if has_uvresidplots:
        # For now...
        dest_uvresidplots = qa_output / uvresidplots_path.name
        uvresidplots_path.replace(dest_uvresidplots)

        print("Making tar file w/ QA plots included.")

        finaltar_file = finalproducts_folder / f"{track_tar.name.rstrip('.tar')}_withQA.tar"
        with tarfile.open(finaltar_file, 'w') as tf_final:

            tf_final.add(qa_output)
