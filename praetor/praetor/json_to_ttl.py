import json

# change to output dir and session id ?

# add open operation to make into dictionary

def generate_agent_triple(agent_file):
    with open(agent_file, 'r') as f:
        agent_json = json.load(f)

    agent_id = agent_json["agent"]["var"]["lifeline"]
    agent_string = """
<{0}> a prov:Agent ;
    prtr:pythonVersion "{1}" ;""".format(agent_json["agent"]["var"]["lifeline"], agent_json["agent"]["var"]["python_version"])
    for key in agent_json["agent"]["var"]:
        if key not in ["lifeline", "python_version"]:
            agent_string += """
    {} "{}" ;""".format(key, agent_json["agent"]["var"][key])
    agent_string = agent_string[:-1] + "."
    return agent_string, agent_id


class Converter:
    context = """@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix run: <http://example.org/> .
@prefix urn_uuid: <urn:uuid:> .
@prefix ivoa: <https://www.ivoa.net/documents/ProvenanceDM/20200411/Provenance.html> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix var: <http://openprovenance.org/var#> .
@prefix prtr: <http://example.org/> .
@prefix pyth: <https://pypi.org/project/> .
"""
    # agent_triple, agent_id = generate_agent_triple(agent_dictionary)
    agent_id = "agent_id"

    def __init__(self, bindings):
        self.triple_string = ""
        self.bindings = bindings
        self.blank_counter = 0

    def generate_activity_triple(self):
        act_string = """
<{0}> a prov:Activity ;
    prov:startedAtTime "{1}"^^{2} ;
    prov:endedAtTime "{3}"^^{4} ;
    prtr:activityName "{5}" ;
    prtr:activitySource "{6}" .

<{0}> prov:wasAssociatedWith <{7}> .""".format(self.bindings['message']["@id"],
                                                       self.bindings["messageStartTime"]["@value"],
                                                       self.bindings["messageStartTime"]["@type"],
                                                       self.bindings["messageEndTime"]["@value"],
                                                       self.bindings["messageEndTime"]["@type"],
                                                       self.bindings['activityName']["@value"],
                                                       self.bindings['moduleName']["@value"],
                                                       self.agent_id)
        self.triple_string += act_string

    def generate_input_triple(self, input_object, activity_id):
        in_string = """
<{0}> a prov:Entity ;
    prov:value "{1}"^^{2} .

_:blank{3} a prov:Usage ;
    prov:entity <{0}> .

<{5}> prov:qualifiedUsage _:blank{3} . 

_:blank{3} prov:hadRole "{4}" .
        """.format(input_object["@id"], input_object["@value"], input_object["@type"], self.blank_counter, input_object["@role"],
                   activity_id)
        self.blank_counter += 1
        self.triple_string += in_string


    def generate_output_triple(self, output_object, activity_id):
        out_string = """
<{0}> a prov:Entity ;
    prov:value "{1}"^^{2} .

<{0}> prov:wasGeneratedBy <{3}> .""".format(output_object["@id"], output_object["@value"], output_object["@type"],
                                              activity_id)

        self.triple_string += out_string

    def generate_started_string(self):
        start_triples = """
_:blank{0} a prov:Start .

<{1}> prov:qualifiedStart _:blank{0} .

_:blank{0} prov:hadActivity <{2}> .""".format(self.blank_counter, self.bindings["message2"]["@id"],
                                              self.bindings["message"]["@id"])
        self.blank_counter += 1
        self.triple_string += start_triples

    def generate_line_triples(self):
        self.generate_activity_triple()
        activity_id = self.bindings['message']["@id"]
        inputs = [x for x in self.bindings if x.startswith('input_')]
        for input_key in inputs:
            self.generate_input_triple(self.bindings[input_key], activity_id)

        outputs = [x for x in self.bindings if x.startswith('output_')]
        for output_key in outputs:
            self.generate_output_triple(self.bindings[output_key], activity_id)

        if "message2" in self.bindings.keys():
            self.generate_started_string()

        return self.triple_string
