import os, json, base64, boto3

firehose = boto3.client('firehose')

print('Loading function')


def recToFirehose(streamRecord):
    ddbRecord = streamRecord['NewImage']
    toFirehose = {}
    for c in ddbRecord:
        toFirehose[c] = next(iter(ddbRecord[c].values()))
    jtoFirehose = json.dumps(toFirehose)
    response = firehose.put_record(
        DeliveryStreamName=os.environ['DeliveryStreamName'],
        Record={
            'Data': jtoFirehose + '\n'
        }
    )
    print(response)

def lambda_handler(event, context):
    for record in event['Records']:
        if (record['eventName']) != 'REMOVE':
            recToFirehose(record['dynamodb'])
    return 'Successfully processed {} records.'.format(len(event['Records']))