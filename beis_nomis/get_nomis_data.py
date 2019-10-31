from nomis import process_config
from nomis import batch_request
from nomis import reformat_nomis_columns
import boto3

TEST = False


if __name__ == "__main__":

    s3 = boto3.resource('s3')

    # Process config
    dataset = "claimantcount"
    bucket = "innovation-mapping-beis"

    config_filename = ("/Users/jklinger/Downloads/"
                       "nesta-config/official_data/"
                       f"{dataset}.config")
    config_details = process_config(config_filename, test=TEST)
    config, geogs_list, dataset_id, date_format = config_details

    # Iterate over geographies
    for igeo, geographies in enumerate(geogs_list):
        print(geographies)
        # Generate tables for this geography/dataset
        done = False
        record_offset=0
        while not done:
            result = batch_request(config,
                                   dataset_id,
                                   geographies,
                                   date_format,
                                   max_api_calls=10,
                                   record_offset=record_offset)
            df, done, record_offset = result
            key = f'{dataset}/{igeo}-{record_offset}.json'
            s3_obj = s3.Object(bucket, key)
            s3_obj.put(Body=df.to_json(orient='records'))
            break
        break
