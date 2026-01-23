import os.path
from collections import Counter
import inspect
import datetime
import uuid

import json
import sys
import os
import sysconfig
import reprlib

import hashlib
import pickle


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

    json_dir = out_directory + '/json/'

    json_total['agent'] = bindings

    with open(json_dir + 'agent_json.json', 'w') as f:
        json.dump(json_total, f)


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

    mod_dict = sys.modules.items()

    for name, module in mod_dict:
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

    def __init__(self, output_directory="./output/", block_list_mod=None, block_list_func=None, cpython=False,
                 bootstrap=False):

        self.calls = {}
        self.session_id = generate_pipeline_id()  # change praetor to name of pipeline
        self.prov_id_counter = Counter()
        self.prov_id_cache = dict()

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

        mods = get_non_builtin_modules_versions()
        get_modules(self.session_id, self.out_directory, mods)

        self.last_activity = {"id": None, "end": None, "start": None, "name": None}
        self.activity_counter = {}
        self.bindings = {}

        self.block_list_modules = block_list_mod
        self.block_list_func = block_list_func
        self.cpython = cpython
        self.bootstrap = bootstrap

    def __call__(self, frame, event, arg):


        if event in ["call", "return"]:
            code = frame.f_code
            func_name = code.co_name
            module_name = frame.f_globals.get("__name__", None)

            if module_name == "set_trace_functioon":
                print("passing my mod")
                return self
            # #
            # Ignore module-level frames
            if func_name == "<module>":
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


            self.name = func_name
            self.module_name = module_name
            self.stack_id = str(id(frame))

            argcount = code.co_argcount
            varnames = code.co_varnames
            inputs = {
                varnames[i]: frame.f_locals.get(varnames[i])
                for i in range(argcount)
            }
            self.inputs = inputs

            if event == "call":

                self.start_time = self.date_time_stamp()
                self.prov_call_in()
                self.dump_json()
                return self

            elif event == "return":
                self.output = arg
                self.end_time = self.date_time_stamp()
                self.prov_call_out()
                self.dump_json()
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
                self.dump_json()
                return self

            if self.cpython and event == "c_return":
                self.end_time = self.date_time_stamp()
                self.output = "None"
                self.prov_call_out()
                self.dump_json()
                return self




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
        date_time = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')
        return date_time

    @staticmethod
    def remove_quotes_from_string(in_string):
        out_string = [x for x in in_string if x not in ['"', "'"]]
        out_string = ''.join(out_string)
        return out_string

    @staticmethod
    def generate_persistent_id(obj):
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

    def find_type(self, value):
        # mod_list = self.praetor_settings_dict["modules"] # need to get from some place, probably enviro var
        if callable(value) or inspect.ismodule(value) or inspect.isclass(value):#  or type(value).__module__ in mod_list:
            name = getattr(value, '__name__', None)
            out_value = f"<callable {name}>"
            prov_type = 'string'
        else:
            out_value = reprlib.repr(value)
            out_value = self.remove_quotes_from_string(out_value)
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
        file_name = self.out_directory + "/big_entities/{}.json".format(object_id)

        if not os.path.isfile(file_name):
            with open(file_name, "w") as f:
                json.dump(value, f)

        return file_name

    def track_call(self):
        stack = inspect.stack()
        caller = 'main'
        for frame in stack:
            if frame.function == "<module>":
                break
            elif frame.function == self.last_activity['name']:
                caller = self.last_activity['name']
                break
        return caller

    def prov_call_in(self):
        # try:
        #     self.activity_counter[self.name] += 1
        #     identifier = self.name + "_{}".format(self.activity_counter[self.name])
        # except KeyError:
        #     self.activity_counter[self.name] = 0
        #     identifier = self.name + "_{}".format(self.activity_counter[self.name])

        self.bindings['{}'.format(self.stack_id)] = {}
        self.bindings['{}'.format(self.stack_id)]['messageStartTime'] = {"@type": "xsd:dateTime", "@value": self.start_time}
        self.bindings['{}'.format(self.stack_id)]['moduleName'] = {"@type": "xsd:string", "@value": self.module_name}
        self.bindings['{}'.format(self.stack_id)]['activityName'] = {"@type": "xsd:string", "@value": self.name}
        self.bindings['{}'.format(self.stack_id)]['message'] = {"@id": "urn_uuid:{}_{}".format(self.session_id, self.stack_id)}


        # add gate to see if it is on the stack
        # stack_function = self.track_call()
        if self.last_activity['id']:
            self.bindings['{}'.format(self.stack_id)]['message2'] = {"@id": "urn_uuid:{}_{}".format(self.last_activity['name'], self.last_activity['id'])}
            self.bindings['{}'.format(self.stack_id)]['message2StartTime'] = {"@type": "xsd:dateTime", "@value":self.last_activity['end']}
            self.bindings['{}'.format(self.stack_id)]['message2EndTime'] = {"@type": "xsd:dateTime", "@value": self.last_activity['start']}


        counter = 0
        for key, value in self.inputs.items():
            in_value, in_type = self.find_type(value)
            in_id = self.generate_persistent_id(value)
            if len(in_value) > 1024:
                in_value = self.large_object_handling(in_value, in_id)
            self.bindings['{}'.format(self.stack_id)]['input_{}'.format(counter)] = {'@id' : self.prefix + in_id, '@value': in_value,
                                                                     '@type': "xsd:{}".format(in_type), '@role': key}
            counter += 1
        # end time need to be recorded after function execution

        self.last_activity['id'] = self.stack_id
        self.last_activity['name'] = self.name
        self.last_activity['end'] = self.start_time
        self.last_activity['start'] = self.start_time


    def prov_call_out(self):

        try:
            self.bindings['{}'.format(self.stack_id)]['messageEndTime'] = {"@type": "xsd:dateTime", "@value": self.end_time}
        except KeyError:
            # print("return but no call")
            self.bindings['{}'.format(self.stack_id)] = {}
            self.bindings['{}'.format(self.stack_id)]['moduleName'] = {"@type": "xsd:string",
                                                                       "@value": self.module_name}
            self.bindings['{}'.format(self.stack_id)]['activityName'] = {"@type": "xsd:string", "@value": self.name}
            self.bindings['{}'.format(self.stack_id)]['message'] = {
                "@id": "urn_uuid:{}_{}".format(self.session_id, self.stack_id)}

            counter = 0
            for key, value in self.inputs.items():
                in_value, in_type = self.find_type(value)
                in_id = self.generate_persistent_id(value)
                if len(in_value) > 1024:
                    in_value = self.large_object_handling(in_value, in_id)
                self.bindings['{}'.format(self.stack_id)]['input_{}'.format(counter)] = {'@id': self.prefix + in_id,
                                                                                         '@value': in_value,
                                                                                         '@type': "xsd:{}".format(
                                                                                             in_type), '@role': key}
                counter += 1

            output_list = [self.output]
            if output_list:
                for i, output_item in enumerate(output_list):
                    out_value, out_type = self.find_type(output_item)
                    # print(out_value)
                    out_id = self.generate_persistent_id(output_item)
                    if len(out_value) > 1024:
                        out_value = self.large_object_handling(out_value, out_id)
                    self.bindings['{}'.format(self.stack_id)]['output_{}'.format(i)] = {
                        '@id': self.prefix + out_id, '@value': out_value,
                        '@type': "xsd:{}".format(out_type)}

        output_list = [self.output]
        if output_list:
            for i, output_item in enumerate(output_list):
                out_value, out_type = self.find_type(output_item)
                out_id = self.generate_persistent_id(output_item)
                if len(out_value) > 1024:
                    out_value = self.large_object_handling(out_value, out_id)
                self.bindings['{}'.format(self.stack_id)]['output_{}'.format(i)] = {'@id' : self.prefix + out_id, '@value': out_value,
                                                                         '@type': "xsd:{}".format(out_type)}

        self.last_activity['id'] = self.stack_id
        self.last_activity['name'] = self.name
        self.last_activity['end'] = self.end_time
        self.last_activity['start'] = self.start_time


    def dump_json(self):
        json_metadata = self.bindings.pop(self.stack_id, None)
        # print(json_metadata)
        json_str = json.dumps(json_metadata)
        self.out_handle.write(json_str + '\n')
        self.out_handle.flush()


    def dump_json_no_pop(self):
        try:
            json_metadata = self.bindings[self.stack_id]
            json_str = json.dumps(json_metadata)
            self.out_handle.write(json_str + '\n')
            self.out_handle.flush()
        except KeyError:
            print("no key found")
            print(self.name)
            print("full stacker", self.stack_id)


    @classmethod
    def close_file(cls):
        if cls.close_file_var and cls.out_handle:
            cls.out_handle.close()
            cls.file_handle = None


# sys.settrace(tracer)