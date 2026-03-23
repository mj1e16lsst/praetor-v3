import argparse
import atexit

from praetor.praetor import CallTracer
import sys
import os


def list_of_strings(arg: str):
    return arg.split(",")


def get_arguments():
    parser = argparse.ArgumentParser(description='Custom praetor settings.')
    parser.add_argument('--praetor-output', required=False, help='Directory for praetor output')
    parser.add_argument('--praetor-function-blacklist', required=False,
                        help='Comma separated list of functions to blacklist',
                        type=list_of_strings)
    parser.add_argument('--praetor-module-blacklist', required=False,
                        help='Comma separated list of modules to blacklist',
                        type=list_of_strings)
    parser.add_argument('--praetor-save-big', required=False, help='Whether to make copies of large values (Boolean)')
    parser.add_argument('--praetor-cpython', required=False, help='Whether to track cpython functions (Boolean)')
    parser.add_argument('--praetor-bootstrap', required=False, help='Whether to track bootstrapped functions (Boolean)')
    parser.add_argument('--praetor-process-monitor', required=False, help='Whether to profile functions (Boolean)')
    parser.add_argument("--praetor-stack-depth", required=False, help='Max Stack depth of praetor (Integer)')
    args, unknown = parser.parse_known_args()

    output_directory = args.praetor_output
    if output_directory is None:
        output_directory = os.getcwd()

    function_blacklist = args.praetor_function_blacklist
    module_blacklist = args.praetor_module_blacklist

    big_entities = args.praetor_save_big
    if big_entities is None:
        big_entities = False

    cpython = args.praetor_cpython
    if cpython is None:
        cpython = False

    bootstrap = args.praetor_bootstrap
    if bootstrap is None:
        bootstrap = False

    process_monitor = args.praetor_process_monitor
    if process_monitor is None:
        process_monitor = False

    stack_depth = args.praetor_stack_depth
    if stack_depth is None:
        stack_depth = 5
    else:
        stack_depth = int(stack_depth) + 4

    return {"out_dir": output_directory, "func_blacklist": function_blacklist, "module_blacklist": module_blacklist,
            "big_entities": big_entities, "cpython": cpython, "bootstrap": bootstrap,
            "process_monitor": process_monitor, "stack_depth": stack_depth}


arg_dict = get_arguments()
tracer = CallTracer(output_directory=arg_dict["out_dir"], slim=True, process_monitor=arg_dict["process_monitor"],
                    block_list_func=arg_dict["func_blacklist"], block_list_mod=arg_dict["module_blacklist"],
                    store_large_values=arg_dict["big_entities"], cpython=True,
                    bootstrap=True, max_depth=arg_dict["stack_depth"])
sys.setprofile(tracer)

atexit.register(tracer.close)