import argparse
import atexit
import sys

from praetor.praetor import CallTracer


def get_output_directory():
    parser = argparse.ArgumentParser(description='Custom praetor settings.')
    parser.add_argument('--praetor-output',required=False, help='Directory for praetor output')
    args = parser.parse_args()
    try:
        output_directory = args.praetor_output
    except AttributeError:
        output_directory = './output'
    return output_directory


out_dir = get_output_directory()
tracer = CallTracer(output_directory=out_dir, only_main=True)
sys.setprofile(tracer)

atexit.register(tracer.close)