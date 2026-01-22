# prov-PRAETOR

## Installation

The software suite has been organised in three parts that are separated in individual directories. 

Please keep in mind that PRAETOR is a software suite that automatically documents the processing of a python workflow. However in practise you may have already a workflow or pipeline in a running environment (conda or container). In that case you want to install the provenance\-generation software within that environment and may want to investigate the provenance information outside that environment.

All of the software can be installed using the following command:

```
pip install praetor
```

There are also some required installs (such as databases, triple stores, and containers) required for individual directories, installation instructions for these are all included in the respective directories.


## Contents
Each of the directories comes with their own installation receipe. A full installation requires 1,2,3, but 1 is compulsary. 

### 1. Produce provenance data (provenance_generation)
The [provenance\_generation](https://gitlab.mpcdf.mpg.de/PRAETOR/prov-PRAETOR_public/-/tree/main/prov-PRAETOR/provenance_generation) directory contains all of the information required install the provenance generation code and run it with your workflow.

### 2. Browse through the data (user_interface)

Provenance exploration can be achieved via the [user_interface](https://gitlab.mpcdf.mpg.de/PRAETOR/prov-PRAETOR_public/-/tree/main/prov-PRAETOR/user_interface).

### 3. Digging deep into the provenance (provenance_queries)

For more advanced and scalable provenance queries, the [provenance_queries](https://gitlab.mpcdf.mpg.de/PRAETOR/prov-PRAETOR_public/-/tree/main/prov-PRAETOR/provenance_queries) directory contains installation instructions and tutorials for querying provenance in both graph databases and triple stores.


## PRAETOE model desciption (provenance_model)

The PRAETOR provenance model is a prov extension, the full details of which can be found at this address - https://praetor.pages.mpcdf.de/prov-PRAETOR_public/

## Advanced provenance and workflow settings

The provenance generation code comes with a lot of space for provenance customisation, including:
- Tracking provenance for imported libraries
- Excluding specified functions or modules from the provenance
- Tracking memory usage statistics
- Global variable tracking
- Tracking opened files
- Adding comments or other metadata to the provenance
- Adding virtual functions

More information can be found [here](https://gitlab.mpcdf.mpg.de/PRAETOR/prov-PRAETOR_public/-/tree/main/prov-PRAETOR/provenance_generation#advanced-options).

In addition, information on attaching and querying for quality metrics within the provenance can be found [here](https://gitlab.mpcdf.mpg.de/PRAETOR/prov-PRAETOR_public/-/blob/main/prov-PRAETOR/provenance_queries/RDF/fuseki/tutorials/adding_and_querying_for_quality_tutorial.ipynb).


