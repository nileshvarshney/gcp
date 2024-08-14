import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run():
    pipeline_options = PipelineOptions(
        project = 'playground-s-11-370f7e52',
        runner = 'DataflowRunner',
        temp_location = 'gs://dataflow-9999/temp',
        region = 'us-central1'
    )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'Read from Text' >> beam.io.ReadFromText('gs://dataflow-9999/input/input.txt')
            | 'Count Words' >> beam.FlatMap(lambda line: line.split()) # Split line
            | 'Pair with 1' >> beam.Map(lambda word : (word, 1))
            | 'Group and Sum' >> beam.CombinePerKey(sum)
            | 'Write to Text' >> beam.io.WriteToText('gs://dataflow-9999/output/output.txt')
        )

if __name__ == "__main__":
    run()