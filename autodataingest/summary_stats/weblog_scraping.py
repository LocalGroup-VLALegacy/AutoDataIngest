
import numpy as np
import pandas as pd
from pathlib import Path
from tqdm import tqdm


def extract_flagging_tables(filename='t1-3.html'):
    '''
    Extract the summary flagging statistics from the weblog.
    '''

    # TODO: Identify which field is which from the html page.
    # this is in the html title for each table but pandas won't extract it right now.
    tables = pd.read_html(filename, header=0, index_col=0, match='spw')

    # Remove the stage error/warning table when present
    return [tab for tab in tables if "Task" not in tab.columns]


def make_flagging_summary_tables(qa_path='bigdata/vlaxl/public_html/data/',
                                 output_path='space/vlaxl/summary_statistics/weblog_flagging_tables/',
                                 overwrite=False,
                                 target_filename='t1-3.html',
                                 skip_nan_cols=True):

    path = Path(qa_path)

    output_path = Path(output_path)
    output_path.mkdir(exist_ok=True)

    # Walk through all t1-3.html files
    for filename in tqdm(path.glob(f"**/{target_filename}")):

        # Extract the products name
        prod_name = [part for part in str(filename).split("/") if "_products" in part]
        assert len(prod_name) == 1
        prod_name = prod_name[0]

        # Extract the tables:
        tables = extract_flagging_tables(filename=filename)

        for ii, this_table in enumerate(tables):
            output_name = f"{prod_name}_field_{ii}.csv"

            out_full_path = output_path / output_name

            # Skip if it exists:
            if not overwrite and out_full_path.exists():
                continue

            if skip_nan_cols:
                this_table[::2].to_csv(out_full_path)
            else:
                this_table.to_csv(out_full_path)


def make_flagging_statistics(project='20A-346',
                             config='all',
                             target='all',
                             data_type='continuum',
                             data_path='space/vlaxl/summary_statistics/weblog_flagging_tables/',
                             finalqa_only=True):
    '''
    Expect file naming scheme:

    TARGET_CONFIG_SDMName

    where the SDM name starts with the project code.
    '''

    path = Path(data_path)

    if target is not "all":
        target_string = f"{target}"
    else:
        target_string = "*"

    if config is not "all":
        config_string = f"{config}"
    else:
        config_string = "*"

    if project is not "all":
        project_string = f"{project}"
    else:
        project_string = ""

    search_string = f"{target_string}_{config_string}_{project_string}"

    table_list = []

    print(f"{search_string}*{data_type}*.csv")

    for filename in path.glob(f"{search_string}*{data_type}*.csv"):

        # Only include if this was the final run for the QA
        if finalqa_only:
            # Grab every instance with the SDM name and data type and find the
            # highest N iteration for the final version.

            sdm_type_name = f"{filename.name.split(data_type)[0]}{data_type}"

            qa_iteration = [str(this_file).split("products")[1][1] for this_file in
                            path.glob(f"{sdm_type_name}*")]
            # Switch the 0th iteration to a number:
            qa_iteration = [val if val != "f"  else "0" for val in qa_iteration]
            qa_iteration = np.array(qa_iteration).astype(int)

            last_iteration = qa_iteration.max()

            if last_iteration == 0:
                check_string = f"{sdm_type_name}_products"
            else:
                check_string = f"{sdm_type_name}_products_{last_iteration}"

            if not check_string in filename.name:
                continue

        this_tab = pd.read_csv(filename, index_col=0,)

        table_list.append(this_tab)

    if len(table_list) == 0:
        print(f"Unable to find any files matching: {search_string}")
        return None

    else:
        all_tab = pd.concat(table_list, axis=1).mean(axis=1)
        return all_tab


def make_config_flagging_summary_plots(project='20A-346',
                                       highflag_line=80.,
                                       new_spw_order=False,
                                       out_name="spw_flagging_summary.png",
                                       finalqa_only=True,
                                       data_type='continuum',
                                       print_stats=True):

    import matplotlib.pyplot as plt

    ax = plt.subplot(111)

    for config in "ABCD":
        tab = make_flagging_statistics(project=project,
                                       target='all',
                                       config=config,
                                       finalqa_only=finalqa_only,
                                       data_type=data_type)

        if print_stats:
            print(f"Config: {config}")
            print(tab)

        if tab is None:
            continue

        xvals = tab.index
        yvals = tab

        if new_spw_order:
            if data_type == "continuum":
                mask = tab.index >= 16.0
            else:
                mask = np.ones(tab.index.shape, dtype=bool)
        else:
            mask = np.ones(tab.index.shape, dtype=bool)

        ax.scatter(xvals[mask], yvals[mask], label=config)

    ax.set_ylim([0., 100.])
    ax.legend(frameon=True)

    ax.set_ylabel("Average Flag Percent")
    ax.set_xlabel("SPW")

    ax.axhline(highflag_line, color='k', linestyle='--')

    ax.grid(True)

    plt.savefig(f"{project}_{data_type}_{out_name}")
    plt.close()
