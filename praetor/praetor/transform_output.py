import json
import os

from praetor import match_json
from praetor import json_to_ttl

def create_full_json(agent_json, main_json):
    root, ext = os.path.splitext(main_json)
    main_ttl = root + '.ttl'
    agent_triple, agent_id = json_to_ttl.generate_agent_triple(agent_json)

    out_json = root + '_flattend.json'
    line_index = match_json.json_concat(main_json)
    output = match_json.read_pairs_adaptive_return(main_json, out_json, line_index)

    with open(main_ttl, "w") as write_to:
        with open(out_json, "r") as to_read:
            counter = 0
            for line in to_read:
                json_line = json.loads(line)
                if counter == 0:
                    converter = json_to_ttl.Converter(json_line)
                    converter.agent_id = agent_id
                    write_to.write(converter.context)
                    write_to.write(agent_triple)
                    counter += 1
                else:
                    converter.bindings = json_line
                    converter.triple_string = ""
                line_triple = converter.generate_line_triples()
                # print(len(line_triple))
                write_to.write(line_triple)
                counter += 1
                # print("##### {} #####".format(counter))

