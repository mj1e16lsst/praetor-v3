import argparse
import atexit

from praetor.praetor import CallTracer
import sys
import os

def get_output_directory():
    parser = argparse.ArgumentParser(description='Custom praetor settings.')
    parser.add_argument('--praetor-output',required=False, help='Directory for praetor output')
    args = parser.parse_args()

    output_directory = args.praetor_output
    if output_directory is None:
        output_directory = os.getcwd()
    return output_directory


out_dir = get_output_directory()
tracer = CallTracer(output_directory=out_dir, slim=True)
sys.setprofile(tracer)

atexit.register(tracer.close)