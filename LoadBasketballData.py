
import os, sys, time, decimal
from decimal import *
import boto3
import json
dynamodb = boto3.resource('dynamodb', region_name='ca-central-1')
table = dynamodb.Table('BasketballStats')



def loadfile(infile):
    jsonobj = json.load(open(infile, encoding="utf8"))
    jsonobj_dump = json.dumps(jsonobj)
    jsonobj = json.loads(jsonobj_dump, parse_float=Decimal)
    lc = 1
    for stat in jsonobj:
        lc += 1
        CreateTime = int(time.time())
        ExpireTime = CreateTime + (1* 60* 60)
        response = table.put_item(
           Item={
                'Rk': stat['Rk'],
                'Name': stat['Name'],
                'Age': stat['Age'],
                'MinutesPlayed': stat['MP'],
                'GamesPlayed': stat['G'],
                'FieldGoalPercent': stat['FG%'],
                'ThreePointFieldGoalPercent':stat['3P%'],
                'FreeThrowPercent': stat['FT%'],
                'PTS': stat['PTS'],
                'CreateTime': CreateTime,
                'ExpireTime': ExpireTime
            }
        )
        if (lc % 10) == 0:
            print ("%d rows inserted" % (lc))

if __name__ == '__main__':

    aws_session = boto3.Session()
    # Create an S3 client
    s3 = aws_session.client('s3')
    filename = sys.argv[1]
    if os.path.exists(filename):
        # file exists, continue
        loadfile(filename)
    else:
        print ('Please enter a valid filename')