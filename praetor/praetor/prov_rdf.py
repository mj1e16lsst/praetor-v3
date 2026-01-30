from collections import Counter
from datetime import datetime
import logging
import os
import requests
import subprocess


import pandas as pd
from pandas import json_normalize

# add function to remove prefixes from results

DATABASE_HOST_URL = os.environ.get('DATABASE_HOST_URL', 'http://127.0.0.1:3030/')
REPOSITORY_ID = os.environ.get('REPOSITORY_ID', 'ds')
SPARQL_REST_URL = DATABASE_HOST_URL + REPOSITORY_ID

prefixes = '''
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX run: <http://example.org/>
        PREFIX exe: <http://example.org/>
        PREFIX prtr: <https://praetor.pages.mpcdf.de/praetor_provenance/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
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

def modules_query(prov_name):
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


def track_functions(prov_name, input_name=None, output_name=None, function_id=None, trace_back=True):
    '''
    Determine what functions have executed before or after a given input value, output value, or function id.
    Note, values may appear more than once, in this case the first instance is always used.
    :param prov_name:
    :param input_name:
    :param output_name:
    :param function_id:
    :param trace_back:
    :return:
    '''
    if input_name:
        find_ba = prefixes + '''
        SELECT ?actID ?time
        FROM NAMED <''' + prov_name + '''>

        WHERE {
        GRAPH ?g {

        ?entityID prov:value "''' + input_name + '''" .
        ?be prov:entity ?entityID .
        ?actID prov:qualifiedUsage ?be .
        ?ba prov:hadActivity ?actID .
        ?actID prov:startedAtTime ?time .
        }
        }
        '''
        ba = user_defined_query(find_ba)
        time = ba['time.value'][0]
    elif output_name:
        find_ba = prefixes + '''
        SELECT ?actID ?time
        FROM NAMED <''' + prov_name + '''>

        WHERE {
        GRAPH ?g {

        ?entityID prov:value "''' + output_name + '''" .
        ?entityID prov:wasGeneratedBy ?actID .
        ?ba prov:hadActivity ?actID .
        ?actID prov:startedAtTime ?time .
        }
        }
        '''
        ba = user_defined_query(find_ba)
        time = ba['time.value'][0]
    elif function_id:
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
        raise Exception("Input error, please enter a value for either the input_name, output_name, or function_id")

    date_time_mine = pd.to_datetime(time)

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
    else:
        df_filtered = all_times[all_times['start.value'] >= date_time_mine]

    return df_filtered