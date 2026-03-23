from collections import Counter
from datetime import datetime
import logging
import os
import requests
import subprocess

import ast
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import numpy as np

import pandas as pd
from pandas import json_normalize

# add function to remove prefixes from results

DATABASE_HOST_URL = os.environ.get('DATABASE_HOST_URL', 'http://127.0.0.1:3030/')
REPOSITORY_ID = os.environ.get('REPOSITORY_ID', 'ds')
SPARQL_REST_URL = DATABASE_HOST_URL + REPOSITORY_ID

prefixes = '''
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX run: <http://example.org/>
PREFIX urn_uuid: <urn:uuid:>
PREFIX ivoa: <https://www.ivoa.net/documents/ProvenanceDM/20200411/Provenance.html>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX var: <http://openprovenance.org/var#>
PREFIX prtr: <http://example.org/> 
PREFIX pyth: <https://pypi.org/project/> 
'''

prefix_dict = {'http://www.w3.org/ns/prov#': 'prov:',
               'http://example.org/': 'run:',
               'https://praetor.pages.mpcdf.de/praetor_provenance/': 'prtr:',
               'http://www.w3.org/2000/01/rdf-schema#': 'rdfs:'}

def clear_response(arr):
    logging.debug('StaticUtils: clearing given array')
    unwanted_strings = ['s', '', ' ', 's,p', 's,p,o', 'p,o', 'o', 'p', 'g', 'g,s,p,o', 's,p,o,r',
                        'activity,entity,p,dt,value,role,startTime,endTime', 's,s_count', 's,s_count,graphs', 's_count',
                        's,s_count,p,o,o_count', 'uuid,s,p,o', 's,g,s_count', 's,s_count,roles', 's,ts,te', 's,u']

    for string in unwanted_strings:
        if string in arr:
            arr.remove(string)

    logging.debug('StaticUtils: cleared list: ' + str(arr))

    return arr


def query_handler(query):
    '''
    Sends query to triple store
    :param query:
    :return: response of query
    '''
    response = requests.post(SPARQL_REST_URL,
                             data={'query': query},
                             timeout=86400)

    return response


def upload_provenance(file_name):
    '''
    Function for uploading provenance files to the database
    :param file_name: name of provenance file to upload
    :return: Name of the graph in the database
    '''
    file_name_short = file_name.split('/')[-1]
    pipeline = 'http://' + file_name_short.replace('.ttl', '')
    data = open(file_name).read()
    headers = {'Content-Type': 'text/turtle;charset=utf-8'}
    url = SPARQL_REST_URL + '/data'
    requests.post(url, params={'graph': pipeline}, data=data, headers=headers)
    return pipeline

def convert_to_datetime_exception(datetime_str):
    '''
    Converts a string representation of datetime to a datetime object - temperamental
    :param datetime_str:
    :return: datetime object
    '''
    try:
        time = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        try:
            time = datetime.strptime(datetime_str[:-6], '%Y-%m-%dT%H:%M:%S.%f')
        except ValueError:
            try:
                time = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S.%fZ')
            except ValueError:
                try:
                    time = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S')
                except:
                    time = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ')


    return time


def result_to_df(response, pre_dict=prefix_dict):
    '''
    Converts a response from the database to a pandas dataframe
    :param response: response to convert
    :return: pandas dataframe format response
    '''
    df = json_normalize(response.json()['results']['bindings'], meta='value')
    cols = [c for c in df.columns if c[-6:] == '.value']
    df2 = df[cols]
    col_names = [x for x in cols]

    if 'startTime.value' in col_names and 'endTime.value' in col_names:
        start_strs = df['startTime.value'].tolist()
        end_strs = df['endTime.value'].tolist()

        start_str = [x for x in start_strs]
        datetime_start = [convert_to_datetime_exception(x) for x in start_str]

        end_str = [x for x in end_strs]
        datetime_end = [convert_to_datetime_exception(x) for x in end_str]

        durations = [(x - y).total_seconds() for x, y in zip(datetime_end, datetime_start)]

        df2.insert(1, 'duration (s)', durations)

    df2 = df2.drop_duplicates()
    for key, value in pre_dict.items():
        df2 = df2.replace(key, value, regex=True)
    return df2


def user_defined_query(query):
    '''
    Function to pass a SPARQL query to the database
    :param query: string SPARQL query
    :return: dataframe of the results
    '''
    res = query_handler(query)
    response_as_string = clear_response(res.text.split("\r\n"))
    # print(response_as_string)
    df = result_to_df(res)
    return df

def modules_query(prov_name, prefixes=prefixes):
    '''
    Function to return module names and versions
    :param prov_name: name of provenance graph
    :return:
    '''

    query = prefixes + '''
SELECT ?modname ?modver
FROM NAMED <''' + prov_name + '''>
WHERE {
GRAPH ?g {

?agentID a prov:Agent ;
    ?modname ?modver .

}
}
'''
    res = query_handler(query)
    df = result_to_df(res)
    return df


def track_functions(prov_name, search_string, function_id=None, trace_back=True, prefixes=prefixes):
    '''
    Determine what functions have executed before or after a given input/output value or function id.
    Searches for partial matches (contains) in input/output values.
    Tries input first, then output if not found as input.

    :param prov_name: Graph to search
    :param search_string: String to search for in input/output values
    :param function_id: Optional exact function ID
    :param trace_back: If True, show functions before; False, show after
    :param prefixes: SPARQL prefixes
    :return: DataFrame of matching functions and times
    '''

    # Try function_id first (exact match)
    if function_id:
        find_ba = prefixes + '''
        SELECT ?time
        FROM NAMED <''' + prov_name + '''>
        WHERE {
        GRAPH ?g {
        <''' + function_id + '''> prov:startedAtTime ?time .
        }
        }
        '''
        ba = user_defined_query(find_ba)
        time = ba['time.value'][0]
    else:
        # Try input first (partial match using REGEX)
        find_input = prefixes + '''
        SELECT ?actID ?time
        FROM NAMED <''' + prov_name + '''>
        WHERE {
        GRAPH ?g {
        ?entityID prov:value ?value .
        FILTER(regex(?value, "''' + search_string + '''", "i")) .
        ?be prov:entity ?entityID .
        ?actID prov:qualifiedUsage ?be .
        ?ba prov:hadActivity ?actID .
        ?actID prov:startedAtTime ?time .
        }
        }
        '''
        input_results = user_defined_query(find_input)

        if not input_results.empty:
            time = input_results['time.value'].iloc[0]  # First match
        else:
            # Try output (partial match)
            find_output = prefixes + '''
            SELECT ?actID ?time
            FROM NAMED <''' + prov_name + '''>
            WHERE {
            GRAPH ?g {
            ?entityID prov:value ?value .
            FILTER(regex(?value, "''' + search_string + '''", "i")) .
            ?entityID prov:wasGeneratedBy ?actID .
            ?ba prov:hadActivity ?actID .
            ?actID prov:startedAtTime ?time .
            }
            }
            '''
            output_results = user_defined_query(find_output)

            if output_results.empty:
                raise Exception(f"No matches found for '{search_string}' in inputs or outputs")

            time = output_results['time.value'].iloc[0]  # First match

    date_time_mine = pd.to_datetime(time)

    # Get all function timings
    get_all_timing = prefixes + '''
    SELECT ?start ?name
    FROM NAMED <''' + prov_name + '''>
    WHERE {
    GRAPH ?g{
    ?actID a prov:Activity .
    ?actID prov:startedAtTime ?start.
    ?actID prtr:activityName ?name .
    }
    }
    '''

    all_times = user_defined_query(get_all_timing)
    all_times['start.value'] = pd.to_datetime(all_times['start.value'])

    if trace_back:
        df_filtered = all_times[all_times['start.value'] <= date_time_mine]
        df_filtered = df_filtered.sort_values(by=['start.value'], ascending=False)
        df_filtered["fileOperation"] =  None
        df_filtered.iloc[-1, df_filtered.columns.get_loc("fileOperation")] = "File operation function"
    else:
        df_filtered = all_times[all_times['start.value'] >= date_time_mine]
        df_filtered = df_filtered.sort_values(by=['start.value'], ascending=True)
        df_filtered["fileOperation"] =  None
        df_filtered.iloc[0, df_filtered.columns.get_loc("fileOperation")] = "File operation function"

    return df_filtered

# def track_functions(prov_name, input_name=None, output_name=None, function_id=None, trace_back=True, prefixes=prefixes):
#     '''
#     Determine what functions have executed before or after a given input value, output value, or function id.
#     Note, values may appear more than once, in this case the first instance is always used.
#     :param prov_name:
#     :param input_name:
#     :param output_name:
#     :param function_id:
#     :param trace_back:
#     :param prefixes:
#     :return:
#     '''
#     if input_name:
#         find_ba = prefixes + '''
#         SELECT ?actID ?time
#         FROM NAMED <''' + prov_name + '''>
#
#         WHERE {
#         GRAPH ?g {
#
#         ?entityID prov:value "''' + input_name + '''" .
#         ?be prov:entity ?entityID .
#         ?actID prov:qualifiedUsage ?be .
#         ?ba prov:hadActivity ?actID .
#         ?actID prov:startedAtTime ?time .
#         }
#         }
#         '''
#         ba = user_defined_query(find_ba)
#         time = ba['time.value'][0]
#     elif output_name:
#         find_ba = prefixes + '''
#         SELECT ?actID ?time
#         FROM NAMED <''' + prov_name + '''>
#
#         WHERE {
#         GRAPH ?g {
#
#         ?entityID prov:value "''' + output_name + '''" .
#         ?entityID prov:wasGeneratedBy ?actID .
#         ?ba prov:hadActivity ?actID .
#         ?actID prov:startedAtTime ?time .
#         }
#         }
#         '''
#         ba = user_defined_query(find_ba)
#         time = ba['time.value'][0]
#     elif function_id:
#         find_ba = prefixes + '''
#         SELECT ?time
#         FROM NAMED <''' + prov_name + '''>
#
#         WHERE {
#         GRAPH ?g {
#         <''' + function_id + '''> prov:startedAtTime ?time .
#         }
#         }
#         '''
#         ba = user_defined_query(find_ba)
#         time = ba['time.value'][0]
#     else:
#         raise Exception("Input error, please enter a value for either the input_name, output_name, or function_id")
#
#     date_time_mine = pd.to_datetime(time)
#
#     get_all_timing = prefixes + '''
#     SELECT ?start ?name
#     FROM NAMED <''' + prov_name + '''>
#     WHERE {
#     GRAPH ?g{
#
#     ?actID a prov:Activity .
#     ?actID prov:startedAtTime ?start.
#     ?actID prtr:activityName ?name .
#     }
#     }
#     '''
#
#     all_times = user_defined_query(get_all_timing)
#     all_times['start.value'] = pd.to_datetime(all_times['start.value'])
#     if trace_back:
#         df_filtered = all_times[all_times['start.value'] <= date_time_mine]
#     else:
#         df_filtered = all_times[all_times['start.value'] >= date_time_mine]
#
#     return df_filtered

def function_query(prov_name, prefixes=prefixes, group_by=None):
    """

    :param prov_name:
    :param prefixes:
    :param group_by:
    :return:
    """

    query = prefixes + '''
    SELECT ?funcID ?funcName ?inputNames ?inputValues ?outputValues 

    FROM NAMED <''' + prov_name + '''>
    WHERE {
    GRAPH ?g {
    
    ?funcID a prov:Activity;
        prtr:activityName ?funcName .
        
    OPTIONAL {
    ?b prov:entity ?inputID .
    ?funcID prov:qualifiedUsage ?b .
    ?b prov:hadRole ?inputNames .
    ?inputID prov:value ?inputValues .
    }
    
    OPTIONAL {
    ?outputID prov:wasGeneratedBy ?funcID .
    ?outputID prov:value ?outputValues . 
    }
    }
    }
    '''

    full_data_frame = user_defined_query(query)

    if group_by:
        groups = full_data_frame[full_data_frame["funcName.value"] == group_by]
    else:
        groups = full_data_frame.sort_values(by="funcName.value", ascending=False)

    return groups


def module_query(prov_name, module_name, prefixes=prefixes):
    """
    Query functions whose activitySource CONTAINS the input module_name (partial match).

    :param prov_name: Graph name to query
    :param module_name: Substring to search for in activitySource
    :param prefixes: SPARQL prefixes
    :return: DataFrame with function names, IDs, start/end times
    """

    query = prefixes + '''
    SELECT ?funcName ?source ?start ?end ?funcID 
    FROM NAMED <''' + prov_name + '''>
    WHERE {
    GRAPH ?g {
    ?funcID a prov:Activity ;
            prtr:activityName ?funcName ;
            prtr:activitySource ?source .
    FILTER(regex(?source, "''' + module_name + '''", "i")) .

    OPTIONAL {
    ?funcID prov:startedAtTime ?start .
    }

    OPTIONAL {
    ?funcID prov:endedAtTime ?end .
    }
    }
    }
    '''
    full_data_frame = user_defined_query(query)
    return full_data_frame

# def module_query(prov_name, module_name, prefixes=prefixes):
#     """
# 
#     :param prov_name:
#     :param module_name:
#     :param prefixes:
#     :return:
#     """
# 
#     query = prefixes + '''
#     SELECT ?funcName ?funcID ?start ?end
#
#     FROM NAMED <''' + prov_name + '''>
#     WHERE {
#     GRAPH ?g {
#
#     ?funcID a prov:Activity ;
#         prtr:activityName ?funcName ;
#         prtr:activitySource "''' + module_name + '''" .
#
#     OPTIONAL {
#     ?funcID prov:startedAtTime ?start .
#     }
#
#     OPTIONAL {
#     ?funcID prov:endedAtTime ?end .
#     }
#
#     }
#     }
#
#     '''
#     full_data_frame = user_defined_query(query)
#     return full_data_frame


def duration_query(prov_name, prefixes=prefixes, function_graph=False, module_graph=False):
    query = prefixes + '''
    SELECT ?start ?end ?funcName ?module

    FROM NAMED <''' + prov_name + '''>
    WHERE {
    GRAPH ?g {

    ?funcID a prov:Activity ;
            prtr:activityName ?funcName ;
            prtr:activitySource ?module .


        ?funcID prov:startedAtTime ?start .



        ?funcID prov:endedAtTime ?end .


        }
        }
    '''
    df = user_defined_query(query)
    duration = df[['start.value', 'end.value']] = df[['start.value', 'end.value']].apply(pd.to_datetime)
    df['time_diff'] = df['end.value'] - df['start.value']
    df["diff_sec"] = df["time_diff"] / pd.Timedelta(seconds=1)

    if function_graph:
        duration_per_name = df.groupby("funcName.value")["diff_sec"].sum().sort_values(ascending=False)

        # 2. Professional bar chart
        fig, ax = plt.subplots(figsize=(12, 6))

        bars = duration_per_name.plot(
            kind="bar",
            ax=ax,
            color=plt.cm.Blues(np.linspace(0.3, 1, len(duration_per_name))),
            edgecolor='navy',
            linewidth=0.8,
            alpha=0.85
        )

        # Professional styling
        ax.set_xlabel("Function Name", fontsize=12, fontweight='bold')
        ax.set_ylabel("Total Duration (seconds)", fontsize=12, fontweight='bold')
        ax.set_title("Total Duration per Function", fontsize=14, fontweight='bold', pad=20)

        # Rotate x-labels for readability
        ax.tick_params(axis='x', rotation=45, labelsize=10)
        ax.tick_params(axis='y', labelsize=11)

        # Grid for better readability
        ax.grid(axis='y', linestyle='--', alpha=0.5, linewidth=0.7)

        # Tight layout with padding
        plt.tight_layout(pad=2.0)

        # Add value labels on bars
        for bar in bars.patches:
            height = bar.get_height()
            ax.text(
                bar.get_x() + bar.get_width() / 2.,
                height + max(duration_per_name) * 0.01,
                f'{height:.1f}s',
                ha='center',
                va='bottom',
                fontsize=9,
                fontweight='bold'
            )

        plt.show()

    if module_graph:
        duration_per_module = df.groupby("module.value")["diff_sec"].sum().sort_values(ascending=False)

        # 2. Professional bar chart
        fig, ax = plt.subplots(figsize=(12, 6))

        bars = duration_per_module.plot(
            kind="bar",
            ax=ax,
            color=plt.cm.Greens(np.linspace(0.3, 1, len(duration_per_module))),
            edgecolor='darkgreen',
            linewidth=0.8,
            alpha=0.85
        )

        # Professional styling
        ax.set_xlabel("Module Name", fontsize=12, fontweight='bold')
        ax.set_ylabel("Total Duration (seconds)", fontsize=12, fontweight='bold')
        ax.set_title("Total Duration per Module", fontsize=14, fontweight='bold', pad=20)

        # Rotate x-labels for readability
        ax.tick_params(axis='x', rotation=45, labelsize=10)
        ax.tick_params(axis='y', labelsize=11)

        # Grid for better readability
        ax.grid(axis='y', linestyle='--', alpha=0.5, linewidth=0.7)

        # Tight layout with padding
        plt.tight_layout(pad=2.0)

        # Add value labels on bars
        for bar in bars.patches:
            height = bar.get_height()
            ax.text(
                bar.get_x() + bar.get_width() / 2.,
                height + max(duration_per_module) * 0.01,
                f'{height:.1f}s',
                ha='center',
                va='bottom',
                fontsize=9,
                fontweight='bold'
            )

        plt.show()

    return df


def pipeline_profiling(stats):
    with open(stats, "r", encoding="utf-8") as f:
        text = f.read()

    py_list = ast.literal_eval(text)

    # Basic style
    plt.style.use('seaborn-v0_8-whitegrid')
    plt.rcParams.update({
        "font.family": "sans-serif",
        "font.size": 10,
        "axes.labelsize": 11,
        "axes.titlesize": 12,
        "legend.fontsize": 9,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
        "axes.linewidth": 0.8,
        "grid.linewidth": 0.4,
        "grid.alpha": 0.4,
        "figure.dpi": 150,
    })

    # Sort and extract
    records = sorted(py_list, key=lambda r: r['timestamp'])
    times = [datetime.fromtimestamp(r['timestamp']) for r in records]
    current_mb = [r['python_memory']['current_mb'] for r in records]
    func_names = [r['function_name'] for r in records]

    fig, (ax_mem, ax_bar) = plt.subplots(
        2, 1,
        figsize=(6.5, 4.5),
        sharex=True,
        gridspec_kw={'height_ratios': [3, 1], 'hspace': 0.05}
    )

    # Top: memory scatter (all points)
    ax_mem.scatter(times, current_mb, s=3, color="#1f77b4", alpha=0.9)
    ax_mem.set_ylabel("Current memory (MB)")
    ax_mem.set_title("Python memory usage and function intervals over time", pad=8)
    ax_mem.grid(axis="y", linestyle="--")
    ax_mem.grid(axis="x")

    if current_mb:
        ymin, ymax = min(current_mb), max(current_mb)
        margin = 0.05 * (ymax - ymin if ymax > ymin else 1.0)
        ax_mem.set_ylim(ymin - margin, ymax + margin)

    # Bottom: horizontal bars
    ax_bar.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    ax_bar.grid(axis="y")
    ax_bar.grid(axis="x", linestyle="--")

    # **NEW: Filter records to exclude None function names BEFORE calculating durations**
    non_none_indices = [i for i, fn in enumerate(func_names) if fn is not None]

    if len(non_none_indices) < 2:
        # Need at least 2 non-None records to calculate durations
        ax_bar.set_xlabel("Time")
        ax_bar.set_yticks([])
        fig.autofmt_xdate()
        plt.tight_layout()
        plt.show()
        return

    # Get filtered data for non-None functions only
    filtered_times = [times[i] for i in non_none_indices]
    filtered_funcs = [func_names[i] for i in non_none_indices]

    # Calculate durations using ONLY non-None function transitions
    start_times = filtered_times[:-1]
    end_times = filtered_times[1:]
    durations_sec = [(e - s).total_seconds() for s, e in zip(start_times, end_times)]
    filtered_start_times = start_times

    # Unique non-None function names
    unique_funcs = list(dict.fromkeys(filtered_funcs))  # preserves order

    # Distinct colors
    tab20 = list(plt.get_cmap("tab20").colors)
    distinct_colors = tab20[::2]
    color_map = {
        fn: distinct_colors[i % len(distinct_colors)]
        for i, fn in enumerate(unique_funcs)
    }

    # Positions and widths
    y_positions = np.arange(len(durations_sec))
    start_times_num = mdates.date2num(filtered_start_times)
    width = [d / (24 * 3600) for d in durations_sec]  # sec -> days

    # Draw bars
    for i, dur in enumerate(durations_sec):
        fn = filtered_funcs[i]  # function at start of interval
        color = color_map[fn]
        ax_bar.barh(
            y_positions[i],
            width[i],
            left=start_times_num[i],
            color=color,
            edgecolor="black",
            linewidth=0.6,
            height=0.95,
        )

    ax_bar.set_xlabel("Time")
    ax_bar.set_yticks([])

    # Legend for non-None functions only
    handles, labels = [], []
    for fn in unique_funcs:
        label = str(fn)
        handles.append(
            plt.Line2D([0], [0], color=color_map[fn], lw=6, solid_capstyle="butt")
        )
        labels.append(label)

    ax_bar.legend(
        handles, labels,
        title="Function",
        frameon=False,
        bbox_to_anchor=(1.02, 1.02),
        loc="upper left",
        borderaxespad=0.,
    )

    fig.autofmt_xdate(rotation=30, ha="right")
    plt.tight_layout()
    plt.show()

def file_access(prov_name, prefixes=prefixes):
    query = prefixes + '''
        SELECT ?funcName ?fileName ?funcID

        FROM NAMED <''' + prov_name + '''>
        WHERE {
        GRAPH ?g {
        
        ?funcID a prov:Activity;
            prtr:activityName ?funcName ;
            prtr:fileAccess ?fileName .
        
    }
    }
    '''
    df = user_defined_query(query)
    return df
