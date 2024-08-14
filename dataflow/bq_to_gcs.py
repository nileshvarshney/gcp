import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run():
    pipeline_options = PipelineOptions(
        project = 'playground-s-11-370f7e52',
        runner = 'DataflowRunner',
        temp_location = 'gs://dataflow-9999/temp',
        region = 'us-central1'
    )

    sql_stmt = "SELECT * FROM `playground-s-11-370f7e52.school.emp`"

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'Read From BQ' >> beam.io.ReadFromBigQuery(query= sql_stmt, use_standard_sql=True)
            #| 'Transform Data' >> beam.Map(lambda row : (row['field_name'], row['value']))
            | 'Write to Text' >> beam.io.WriteToText(
                'gs://dataflow-9999/output/results',
                file_name_suffix='.csv',
                header = 'empno,name'
            )

        )   


if __name__ == "__main__":
    run()