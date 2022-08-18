"""this dummy DAG is to find out how to solve
the broken job completion status"""
import argparse
import logging
import time
import apache_beam as beam
from apache_beam import Create
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


class SleepBeamDoFn(beam.DoFn):
    def __init__(self, sleep):
        self.sleep = sleep

    def to_runner_api_parameter(self, unused_context):
        pass

    def process(self, element, *args, **kwargs):
        print(f"Processing element {element}.")
        print(f"Sleeping for {self.sleep} seconds...")
        time.sleep(self.sleep)
        print("Done with sleeping.")


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sleep",
        dest="sleep",
        required=True,
        type=int,
        help="amount of seconds to sleep.",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)
    print(known_args.sleep)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    p | "create dummy events" >> Create([1]) | "sleep" >> beam.ParDo(
        SleepBeamDoFn(known_args.sleep)
    )

    run()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()

