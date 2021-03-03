import os, json, base64, boto3

firehose = boto3.client('firehose')

print('Loading function')


def recToFirehose(streamRecord):
    ddbRecord = streamRecord['NewImage']
    toFirehose = {}
    for c in ddbRecord:
        toFirehose[c] = next(iter(ddbRecord[c].values()))
    jddbRecord = json.loads(ddbRecord['info']['S'])
    # Transform the record a bit
    try:
        name = jddbRecord['Name']
    except:
        name = ' '
    try:
        games_played = jddbRecord['G']
    except:
        games_played = ' '
    try:
        minutes_played = jddbRecord['MP']
    except:
        minutes_played = ' '
    try:
        field_goal_percent = jddbRecord['FG%']
    except:
        field_goal_percent = ' '
    try:
        three_point_percent = jddbRecord['3P%']
    except:
        three_point_percent = ' '

    toFirehose["Name"] = name
    toFirehose["G"] = games_played
    toFirehose["MP"] = minutes_played
    toFirehose["FG%"] = field_goal_percent
    toFirehose["3P%"] = three_point_percent
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