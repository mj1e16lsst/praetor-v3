# prov-PRAETOR

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


Add the praetor tracer to the start of your script (after imports)

ouput_directory - where the provenance will be generated
block_list_mod - is a list of modules which will be ignored by the provenance tracker


```
from praetor.praetor import CallTracer
import sys

tracer = CallTracer(output_directory="./output",  block_list_mod=["numpy"])
sys.setprofile(tracer)
```


## Transformation

Once executed, the code will now create a json directory within the output directory and two files within that: the agent file and main file.

The following command should be used to merge the two files and transform into turtle format:

```create_tt.py --main main_file_name.json --agent agent_json.json```

