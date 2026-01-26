import json

def json_concat(input_json):
    id_dict = {}
    with open(input_json, "r") as to_read:
        counter = 0
        for line in to_read:
            line = line.strip()
            if not line:
                continue
            json_line = json.loads(line)
            id_ = json_line["@id"]
            mode_ = json_line["@mode"]
            try:
                id_dict[id_][mode_] = counter
            except KeyError:
                id_dict[id_] = {mode_: counter}
            counter += 1

    return id_dict



def read_pairs_adaptive_return(file_path, output_path, line_index_dict):
    """
    Automatically adapts return format based on call/return presence:
    - Only call: returns just the call line as string
    - Only return: returns just the return line as string
    - Both: returns combined string "call_data | return_data"
    """
    # 1. Analyze structure first
    print("=== STRUCTURE ANALYSIS ===")
    status_map = {}
    needed_lines = set()

    for key, info in line_index_dict.items():
        has_call = "call" in info
        has_return = "return" in info
        status_map[
            key] = "both" if has_call and has_return else "call" if has_call else "return" if has_return else "empty"

        if has_call:
            needed_lines.add(info["call"])
        if has_return:
            needed_lines.add(info["return"])

        status_name = {"both": "CALL AND RETURN", "call": "CALL only", "return": "RETURN only", "empty": "EMPTY"}[
            status_map[key]]
        print(f"Key '{key}': {status_name}")


    if not needed_lines:
        print("No valid line numbers - returning empty dict")
        return {}

    # 2. Read only needed lines (single sequential pass)
    print("Reading file...")
    needed_lines = sorted(needed_lines)
    lines_cache = {}
    current_needed_idx = 0

    with open(output_path, "w", encoding='utf-8') as out_file:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f):
                while (current_needed_idx < len(needed_lines) and
                       needed_lines[current_needed_idx] < line_num):
                    current_needed_idx += 1

                if (current_needed_idx < len(needed_lines) and
                        needed_lines[current_needed_idx] == line_num):
                    lines_cache[line_num] = line.rstrip()
                    current_needed_idx += 1
                    if current_needed_idx >= len(needed_lines):
                        break

        print(f"Read {len(lines_cache)} unique lines\n")

        # 3. Adaptive return based on what's present for each key

        for key, info in line_index_dict.items():
            status = status_map[key]

            if status == "both":
                call_line = info["call"]
                return_line = info["return"]
                if call_line in lines_cache and return_line in lines_cache:
                    json_line_call = json.loads(lines_cache[call_line])["@data"]
                    json_line_return = json.loads(lines_cache[return_line])["@data"]
                    merged = json_line_return | json_line_call # need to change for earlier python versions
                    result_line = merged
                else:
                    result_line = "MISSING_LINE_DATA"

            elif status == "call":
                call_line = info["call"]
                if call_line in lines_cache:
                    json_line = json.loads(lines_cache[call_line])["@data"]
                    result_line = json_line
                else:
                    result_line = "MISSING_LINE_DATA"

            elif status == "return":
                return_line = info["return"]
                if return_line in lines_cache:
                    json_line = json.loads(lines_cache[return_line])["@data"]
                    result_line = json_line
                else:
                    result_line = "MISSING_LINE_DATA"

            out_file.write(json.dumps(result_line) + "\n")
            out_file.flush()
