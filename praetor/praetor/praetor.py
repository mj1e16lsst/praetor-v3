import os.path
from collections import Counter
import inspect
import datetime
import uuid

import json
import sys
import os
import shutil
import sysconfig
import reprlib

import hashlib
import pickle

from praetor.transform_output import create_full_json
from praetor.process_monitor import DynamicProcessMonitor

custom_repr = reprlib.Repr()
custom_repr.maxlist = 80
custom_repr.maxdict = 40
custom_repr.maxstring = 1024
custom_repr.maxlevel = 3  # nesting depth


def get_caller_script_name():
    top_script_path = getattr(sys.modules['__main__'], '__file__', None)
    if top_script_path is not None:
        top_script_name = os.path.basename(top_script_path)
        return "".join(top_script_name.split('.')[:-1])
    else:
        return "python"


def generate_pipeline_id():
    """
    Creates a unique id for a pipeline run which includes the praetor version
    :return: Unique id
    """
    pipeline_id = "{}_provenance_{}".format(get_caller_script_name(), uuid.uuid4())
    return pipeline_id

def get_modules(pipeline_id, out_directory, modules):
    '''
    Function to create the agent_json.json file, including creating a unique id for the agent, determining all modules
    imported, find their versions, structure and input all infroamtion into agent_json.json
    :return: agent_json.json
    '''
    bindings = {
        "context": {"xsd": "http://www.w3.org/2001/XMLSchema#", "lsim": "http://example.org/", "urn_uuid": "urn:uuid:",
                    "run": "http://example.org/", "pyth" : "https://pypi.org/project"}}


    json_total = {}
    py_version = sys.version.split()[0]
    modules_pre = {f"pyth:{key}": value for key, value in modules.items()}
    bindings['var'] = {}
    bindings['var'].update(modules_pre)
    bindings['var']['python_version'] = py_version
    bindings['var']['lifeline'] = 'urn_uuid:{}'.format(pipeline_id)
    bindings['var']['run_cmd'] = f"{sys.executable} {' '.join(sys.argv)}"

    json_dir = out_directory + '/json/'

    json_total['agent'] = bindings

    agent_json = json_dir + "agent_json.json"

    with open(agent_json, 'w') as f:
        json.dump(json_total, f)

    return agent_json


def get_stdlib_modules():
    stdlib_path = sysconfig.get_paths().get('stdlib')
    module_names = set()

    for root, dirs, files in os.walk(stdlib_path):
        if '__pycache__' in dirs:
            dirs.remove('__pycache__')

        for filename in files:
            if filename.endswith('.py'):
                mod_name = filename[:-3]
                if mod_name == '__init__':
                    continue
                rel_dir = os.path.relpath(root, stdlib_path)
                if rel_dir == '.':
                    full_mod_name = mod_name
                else:
                    full_mod_name = rel_dir.replace(os.sep, '.') + '.' + mod_name
                module_names.add(full_mod_name)

        for dirname in dirs:
            if os.path.isfile(os.path.join(root, dirname, '__init__.py')):
                rel_dir = os.path.relpath(root, stdlib_path)
                if rel_dir == '.':
                    full_pkg_name = dirname
                else:
                    full_pkg_name = rel_dir.replace(os.sep, '.') + '.' + dirname
                module_names.add(full_pkg_name)
    return module_names


def get_non_builtin_modules_versions():
    builtin_modules = set(sys.builtin_module_names)
    stdlib_modules = get_stdlib_modules()
    modules_versions = {}

    mod_dict = dict(sys.modules)

    for name, module in mod_dict.items():
        if (module is not None and
            name not in builtin_modules and
            name not in stdlib_modules and
            not name.startswith('_') and
            '.' not in name and name != "praetor"):  # exclude submodules
            version = getattr(module, '__version__', None)
            if version is None:
                version = getattr(module, 'VERSION', None)
                if isinstance(version, tuple):
                    version = '.'.join(map(str, version))
            modules_versions[name] = version
    return modules_versions



class CallTracer:

    close_file_var = True
    out_handle = None
    agent_json = None

    def __init__(self, output_directory="./output/", block_list_mod=None, block_list_func=None, cpython=False,
                 bootstrap=False, store_large_values=False, only_main=False, slim=False, monitor_interval=1.0,
                 process_monitor=False):
        """Setup for the CallTracer class
        :param output_directory: Where generated provenance will be stored
        :param block_list_mod: block_list of python modules
        :param block_list_func: block_list of python functions
        :param cpython: Whether to record provenance for cpython functions
        :param bootstrap: Whether to record provenance for bootstrapped functions
        :param store_large_values: Whether to store copies of large values as files"""

        self.record_prov = True
        self.process_monitor = process_monitor
        if self.process_monitor:
            self.monitor = DynamicProcessMonitor(base_interval=monitor_interval)

        self.calls = {}
        self.session_id = generate_pipeline_id()  # change praetor to name of pipeline
        self.prov_id_counter = Counter()
        self.prov_id_cache = dict()

        self.store_large_values = store_large_values
        self.prefix = "run:"
        self.dtypes = {'int': 'int', 'unsignedInt': 'unsignedInt', 'hexBinary': 'hexBinary', 'NOTATION': 'NOTATION',
                  'nonPositiveInteger': 'nonPositiveInteger', 'float': 'float', 'ENTITY': 'ENTITY', 'bool': 'boolean',
                  'positiveInteger': 'positiveInteger', 'duration': 'duration', 'IDREFS': 'IDREFS',
                  'unsignedLong': 'unsignedLong', 'normalizedString': 'normalizedString',
                  'dateTimeStamp': 'dateTimeStamp',
                  'NMTOKEN': 'NMTOKEN', 'negativeInteger': 'negativeInteger', 'base64Binary': 'base64Binary',
                  'long': 'long', 'unsignedShort': 'unsignedShort', 'ENTITIES': 'ENTITIES', 'anyURI': 'anyURI',
                  'NMTOKENS': 'NMTOKENS', 'IDREF': 'IDREF', 'unsignedByte': 'unsignedByte', 'Name': 'Name',
                  'dayTimeDuration': 'dayTimeDuration', 'date': 'date', 'integer': 'integer', 'byte': 'byte',
                  'ID': 'ID',
                  'gMonth': 'gMonth', 'short': 'short', 'language': 'language', 'gMonthDay': 'gMonthDay',
                  'double': 'double', 'Decimal': 'decimal', 'gDay': 'gDay', 'gYearMonth': 'gYearMonth',
                  'QName': 'QName', 'datetime': 'dateTime', 'nonNegativeInteger': 'nonNegativeInteger',
                  'gYear': 'gYear',
                  'token': 'token', 'time': 'time', 'yearMonthDuration': 'yearMonthDuration', 'NCName': 'NCName',
                  'str': 'string'}
        self.out_directory = output_directory
        if not self.out_directory.endswith("/"):
            self.out_directory += "/"

        os.makedirs(self.out_directory + "json/", exist_ok=True)
        os.makedirs(self.out_directory + "big_entities/", exist_ok=True)
        self.out_handle = open(self.out_directory + "json/" + self.session_id + ".json", "a")

        script_path = os.path.abspath(__file__)
        script_name = script_path.split("/")[-1]
        shutil.copy(script_path, self.out_directory + "{}_{}".format(self.session_id, script_name))

        mods = get_non_builtin_modules_versions()
        self.agent_json = get_modules(self.session_id, self.out_directory, mods)

        self.last_activity = {"id": None, "end": None, "start": None, "name": None}
        self.activity_counter = {}
        self.bindings = {}

        self.block_list_modules = block_list_mod
        self.block_list_func = block_list_func
        self.cpython = cpython
        self.bootstrap = bootstrap
        self.only_main = only_main
        self.slim = slim
        self.prefixes = ("_", "<")

    def __call__(self, frame, event, arg):
        """Method to record metadata for each python call event
        :param frame: Python frame
        :param event: Type of frame
        :param arg: Argument of frame
        :returns self"""

        if event in ["call", "return"]:
            code = frame.f_code
            func_name = code.co_name
            module_name = frame.f_globals.get("__name__", None)

            if self.only_main and module_name != "__main__":
                return self

            if self.slim:
                if func_name.startswith(self.prefixes):
                    return self
                if "._" in module_name:
                    return self

            if module_name == "praetor":
                # print("passing my mod")
                return self

            if "praetor" in module_name:
                # print("passing module")
                return self
            # #
            # Ignore module-level frames
            if func_name == "<module>":
                return self

            if not self.record_prov:
                return self

            if self.bootstrap and module_name in ["importlib._bootstrap_external", "importlib._bootstrap"]:
                return self

            if self.block_list_modules:
                if module_name in self.block_list_modules:
                    return self
                for mod in self.block_list_modules:
                    if module_name.startswith(mod):
                        return self


            if self.block_list_func:
                if func_name in self.block_list_func:
                    return self

            argcount = code.co_argcount
            varnames = code.co_varnames


            if self.slim and varnames[0] in ["cls", "self"]:
                return self

            inputs = {
                varnames[i]: frame.f_locals.get(varnames[i])
                for i in range(argcount)
            }

            self.name = func_name
            self.module_name = module_name
            self.stack_id = str(id(frame))


            self.inputs = inputs

            if self.process_monitor:
                self.monitor.high_freq_snapshot()

            if event == "call":

                self.start_time = self.date_time_stamp()
                self.prov_call_in()
                self.dump_json(mode="call")
                return self

            elif event == "return":
                self.output = arg
                self.end_time = self.date_time_stamp()
                self.prov_call_out()
                self.dump_json(mode="return")
                return self

        elif event in ["c_call", "c_return"]:

            cfunc = arg
            key = str(id(cfunc))
            func_name = getattr(cfunc, "__name__", None)
            module_name = getattr(cfunc, "__module__", None)

            self.name = func_name
            self.module_name = module_name
            self.stack_id = key

            code = frame.f_code
            argcount = code.co_argcount
            varnames = code.co_varnames
            inputs = {
                varnames[i]: frame.f_locals.get(varnames[i])
                for i in range(argcount)
            }
            self.inputs = inputs

            if self.cpython and event == "c_call":
                self.start_time = self.date_time_stamp()
                self.prov_call_in()
                self.dump_json(mode="call")
                return self

            if self.cpython and event == "c_return":
                self.end_time = self.date_time_stamp()
                self.output = "None"
                self.prov_call_out()
                self.dump_json(mode="return")
                return self

        # code for if events are to be added
        # elif event == "exception":
        #     meta = self.calls.get(id(frame))
        #     if meta is not None:
        #         # arg is (exc_type, exc_value, traceback)
        #         exc_type, exc_value, _ = arg
        #         meta["exception"] = exc_value
        #     return self

        return self

    @staticmethod
    def date_time_stamp():
        """Generate current date and time stamp
        :returns: date and time stamp """
        date_time = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')
        return date_time

    @staticmethod
    def remove_quotes_from_string(in_string):
        """Remove quotes from a string
        :param in_string: input string
        :returns in_string without quotes (out_string)"""
        out_string = [x for x in in_string if x not in ['"', "'"]]
        out_string = ''.join(out_string)
        return out_string

    @staticmethod
    def generate_persistent_id(obj):
        """Generate persistent id for python objects so that they are consistent across runs
        :param obj: python object
        :return: persistent id """
        try:
            # Try to serialize to JSON, for JSON serializable objects
            serialized = json.dumps(obj, sort_keys=True).encode('utf-8')
        except (TypeError, OverflowError):
            # Fallback: use pickle for other Python objects
            try:
                serialized = pickle.dumps(obj)
            except (pickle.PicklingError, TypeError, AttributeError): # just except all?
                return str(id(obj))
        # Generate SHA-256 hash of serialized representation
        persistent_id = hashlib.sha256(serialized).hexdigest()
        return persistent_id

    def gen_identifier(self, variable, naming_template="entity"):
        """
        General utility function to create prov ids for python objects
        :param variable: Value of object to create id for
        :param naming_template: Name related to the object in question to include in the id
        :return: ID for target object
        """
        try:
            prov_id = self.prov_id_cache[self.generate_persistent_id(variable)] # change back to id if too slow
        except KeyError:
            self.prov_id_counter[naming_template] += 1
            prov_id = '{}_{}_{}'.format(naming_template, self.session_id, self.prov_id_counter[naming_template])
            self.prov_id_cache[self.generate_persistent_id(variable)] = prov_id # change back to id if too slow

        return prov_id

    def find_type(self, value, value_id):
        """Determine the type of any input python object
        :param value: Input python object
        :param value_id: ID of the input python object
        :return: Type and truncated string representation of the input python object
        """
        # mod_list = self.praetor_settings_dict["modules"] # need to get from some place, probably enviro var
        if callable(value) or inspect.ismodule(value) or inspect.isclass(value):#  or type(value).__module__ in mod_list:
            name = getattr(value, '__name__', None)
            out_value = f"<callable {name}>"
            prov_type = 'string'
        else:
            out_value = custom_repr.repr(value) # truncating the value, need to not do that
            try:
                out_value_full = json.dumps(value, ensure_ascii=False)
            except (TypeError, OverflowError):
                out_value_full = out_value
            out_value = self.remove_quotes_from_string(out_value)
            # print(out_value, out_value_full)
            if self.store_large_values:
                if len(out_value_full) > 1024:
                    self.large_object_handling(out_value_full, value_id)
            try:
                out_type = type(value).__name__
                if out_type in self.dtypes.keys():
                    prov_type = self.dtypes[out_type]
                else:
                    prov_type = 'string'
            except:
                prov_type = 'string'
        return out_value, prov_type

    def large_object_handling(self, value, object_id):
        """Save large entities to file to save on provenance size
        :param value: Value of object to save
        :param object_id: ID of object to save
        :return: Name of file which was dumped to
        """
        file_name = self.out_directory + "/big_entities/{}.json".format(object_id.split(":")[1])

        if not os.path.isfile(file_name):
            with open(file_name, "w") as f:
                json.dump(value, f)

        return file_name

    def track_call(self):
        """Trace the python stack to determine if a function was called by another"""
        stack = inspect.stack()
        caller = 'main'
        for frame in stack:
            if frame.function == "<module>":
                break
            elif frame.function == self.last_activity['name']:
                caller = self.last_activity['name']
                break
        return caller

    def start_monitoring(self):
        """Start continuous background monitoring"""
        self.monitor.start()

    def stop_monitoring(self):
        """Stop continuous background monitoring"""
        self.monitor.stop()

    def get_full_trace(self):
        """Get all trace data with memory snapshots"""
        return self.monitor.get_stats()

    def clear(self):
        """Reset all data"""
        self.metadata.clear()
        self.monitor.clear()

    def prov_call_in(self):
        """Format call metadata into json format provenance"""
        self.bindings['{}'.format(self.stack_id)] = {}
        self.bindings['{}'.format(self.stack_id)]['messageStartTime'] = {"@type": "xsd:dateTime", "@value": self.start_time}
        self.bindings['{}'.format(self.stack_id)]['moduleName'] = {"@type": "xsd:string", "@value": self.module_name}
        self.bindings['{}'.format(self.stack_id)]['activityName'] = {"@type": "xsd:string", "@value": self.name}
        self.bindings['{}'.format(self.stack_id)]['message'] = {"@id": "urn_uuid:{}_{}".format(self.session_id, self.stack_id)}


        # add gate to see if it is on the stack
        # stack_function = self.track_call()
        if self.last_activity['id']:
            self.bindings['{}'.format(self.stack_id)]['message2'] = {"@id": "urn_uuid:{}_{}".format(self.session_id, self.last_activity['id'])}
            self.bindings['{}'.format(self.stack_id)]['message2StartTime'] = {"@type": "xsd:dateTime", "@value":self.last_activity['end']}
            self.bindings['{}'.format(self.stack_id)]['message2EndTime'] = {"@type": "xsd:dateTime", "@value": self.last_activity['start']}


        counter = 0
        for key, value in self.inputs.items():
            in_id = self.generate_persistent_id(value)
            in_value, in_type = self.find_type(value, self.prefix + in_id)
            self.bindings['{}'.format(self.stack_id)]['input_{}'.format(counter)] = {'@id' : self.prefix + in_id, '@value': in_value,
                                                                     '@type': "xsd:{}".format(in_type), '@role': key}
            counter += 1

        self.last_activity['id'] = self.stack_id
        self.last_activity['name'] = self.name
        self.last_activity['end'] = self.start_time
        self.last_activity['start'] = self.start_time


    def prov_call_out(self):
        """Format return metadata into json format provenance"""
        self.bindings['{}'.format(self.stack_id)] = {}
        self.bindings['{}'.format(self.stack_id)]['messageEndTime'] = {"@type": "xsd:dateTime", "@value": self.end_time}
        self.bindings['{}'.format(self.stack_id)]['moduleName'] = {"@type": "xsd:string",
                                                                   "@value": self.module_name}
        self.bindings['{}'.format(self.stack_id)]['activityName'] = {"@type": "xsd:string", "@value": self.name}
        self.bindings['{}'.format(self.stack_id)]['message'] = {
            "@id": "urn_uuid:{}_{}".format(self.session_id, self.stack_id)}

        counter = 0
        for key, value in self.inputs.items():
            in_id = self.generate_persistent_id(value)
            in_value, in_type = self.find_type(value, self.prefix + in_id)
            self.bindings['{}'.format(self.stack_id)]['input_{}'.format(counter)] = {'@id': self.prefix + in_id,
                                                                                     '@value': in_value,
                                                                                     '@type': "xsd:{}".format(
                                                                                         in_type), '@role': key}
            counter += 1

        output_list = [self.output]
        if output_list:
            for i, output_item in enumerate(output_list):
                out_id = self.generate_persistent_id(output_item)
                out_value, out_type = self.find_type(output_item, self.prefix + out_id)
                self.bindings['{}'.format(self.stack_id)]['output_{}'.format(i)] = {
                    '@id': self.prefix + out_id, '@value': out_value,
                    '@type': "xsd:{}".format(out_type)}

        if self.last_activity['id']:
            self.bindings['{}'.format(self.stack_id)]['message2'] = {"@id": "urn_uuid:{}_{}".format(self.session_id, self.last_activity['id'])}
            self.bindings['{}'.format(self.stack_id)]['message2StartTime'] = {"@type": "xsd:dateTime", "@value":self.last_activity['end']}
            self.bindings['{}'.format(self.stack_id)]['message2EndTime'] = {"@type": "xsd:dateTime", "@value": self.last_activity['start']}


        self.last_activity['id'] = self.stack_id
        self.last_activity['name'] = self.name
        try:
            self.last_activity['end'] = self.end_time
        except AttributeError:
            self.last_activity['end'] = "-"
        try:
            self.last_activity['start'] = self.start_time
        except AttributeError:
            self.last_activity['start'] = "-"

    def dump_json(self, mode):
        """dump json provenance to file
        :param mode: call or return"""
        json_metadata = self.bindings.pop(self.stack_id, None)
        # print(json_metadata)
        new_json = {'@id': '{}'.format(self.stack_id), '@mode': mode, '@data': json_metadata}
        json_str = json.dumps(new_json)
        self.out_handle.write(json_str + '\n')
        self.out_handle.flush()


    def close(self):
        """Close json dump document at end of provenance generation"""
        if self.close_file_var and self.out_handle:
            self.stop_monitoring()
            self.record_prov = False
            with open(self.out_directory + "stats.txt", "w") as f:
                f.write(str(self.monitor.get_stats()))
            create_full_json(self.agent_json, self.out_handle.name)
            self.out_handle.close()
            self.close_file_var = False

