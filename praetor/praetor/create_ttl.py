import argparse
import json
import os

import json_to_ttl

def get_file_names():
    parser = argparse.ArgumentParser(description="Run with custom settings file.")
    parser.add_argument("--agent", required=True, help="Path to the agent json file, named agent_json.json in your output directory")
    parser.add_argument("--main", required=True, help="Path to the main json settings file, named with your run id, a uuid, found in the out directory")
    args = parser.parse_args()
    return args.main, args.agent

main_json, agent_json = get_file_names()
root, ext = os.path.splitext(main_json)
main_ttl = root +'.ttl'
agent_triple, agent_id = json_to_ttl.generate_agent_triple(agent_json)

with open(main_ttl, "w") as write_to:
    with open(main_json, "r") as to_read:
        counter = 0
        for line in to_read:
            json_line = json.loads(line)["var"]
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
            print(len(line_triple))
            write_to.write(line_triple)
            counter += 1
            print("##### {} #####".format(counter))