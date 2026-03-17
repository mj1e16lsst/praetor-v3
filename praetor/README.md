**# prov-PRAETOR

The repository contains the PRAETOR python package - a module for automatically generating PROV format provenance 
for any input python file. PRAETOR records function level provenance; the inputs, outputs, timing, names, origins, and
links between any function executed within the python script. Note, function executions that happen outside of python 
such as bootstrapped functions or calls made in software that exist outside of python will not be recorded.

For a detailed example of generation, visualisation, and querying of the provenance please see the [demo notebook](https://github.com/mj1e16lsst/praetor-v3/blob/main/demo/ska-test-case-demo.ipynb)

## Installation

All the software can be installed using the following command:

```
pip install praetor
```

## Database Installtion

The database is installed via docker, run as a standalone container and accessible via requests.

Pull container:

```docker pull secoresearch/fuseki```

Run command:

```docker run --rm -it -p 3030:3030 --name fuseki -e ADMIN_PASSWORD=admin -e ENABLE_DATA_WRITE=true -e ENABLE_UPDATE=true -e ENABLE_UPLOAD=true -e QUERY_TIMEOUT=60000 secoresearch/fuseki```


The database will now be available at localhost:3030

## Usage 

To record and create the provenance for your python pipeline, simply import the module at the start of your script.
There are multiple levels of granularity available for the provenance, the slim setting is recommended:

```python
from praetor import praetor_slim
```

This import will only record the core python functionality and excludes things such as:
- dunder functions such as __init__
- hidden functions (those which start with an underscore)
- cpython funcitons 

After adding the import line, simply run your script as normal, e.g.:

```commandline
python my_script.py
```

After the script has run, the provenance output will be put in the run directory (to change the output location please 
see the optional command line inputs). The output should include:
- /json, the directory containing the JSON files created during the operation
- /big_entities, the directory containing any large entities (default is larger than 1024 chars) if storing is enabled
- my_scipt_provenance_uuid_flattend.json, final json format provenance file
- my_script_provenance.ttl, final ttl format provenance file (for database upload)
- A copy of the original script ran for reproducibility

In addition to the praetor_slim module, there are also the praetor_complete and praetor_main_only modules.
Both are used in the same way as slim i.e. importing the module at the start of your script.
The praetor_complete module will record all python calls which may be useful in some instances but can quickly inflate
the size of the provenance. The praetor_main_only module only records provenance for functions run from the main script
and not for any imported modules, bootstrapped functions, or cpython functions. 

### Command line options
--praetor-output (designate directory to store output files, e.g. --praetor-output ./output)
--praetor-function-blacklist (comma-separated list of function names to blacklist from tracking, e.g. --praetor-function-blacklist "func_a,func_b,func_c").
--praetor-module-blacklist (comma-separated list of module names to blacklist from tracking, e.g. --praetor-module-blacklist "module_a,module_b").
--praetor-save-big (whether to make copies of large values, Boolean, e.g. --praetor-save-big True).
--praetor-cpython (whether to track CPython functions, Boolean, e.g. --praetor-cpython True).
--praetor-bootstrap (whether to track bootstrapped functions, Boolean, e.g. --praetor-bootstrap False).
--praetor-process-monitor (whether to enable process-level profiling/monitoring of functions, Boolean, e.g. --praetor-process-monitor True).