import json
import random
import string
import boto3
from datetime import datetime
from io import StringIO

# ROOT FUNCTION (wellcome)
def home(event, context):
    thisMoment=str(datetime.now())
    body = {
        "message": "Wellcome to commentAPI v1.0! Home function executed successfully!",
        "datetime":thisMoment,
        "input": event
    }
    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }
    return response

# HELLO USER FUNCTION
def helloUser(event, context):
    print("pathParameters: ",event['pathParameters'])
    user=event['pathParameters']['name']
    user=user.replace('%20',' ')
    body = {
        "message": f"Hello user {user}, wellcome to commentAPI!"
    }
    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }
    return response

# CREATE COMMENT FUNCTION (SAVE IN DB)
def createComment(event, context):
    try:
        data=json.loads(event['body'])
        print('Data recived: ',data)
    except:
        print('[Error 30]. body data not found')
        raise

    statusCodeSuccess=201
    msgSuccess='Comment added succesffully'
    date_time=datetime.now()
    letters = string.digits
    chain= ''.join(random.choice(letters) for i in range(7))
    id='CI'+chain
    print('Id generated: {}'.format(id))

    try:
        sentiment=getSentiment(data['comment'],'es')
    except:
        print('[Error 26] service comprehend not available')
        sentiment='UNKNOW'
    print('sentiment detected: ',sentiment)

    try:
        dynamodb=boto3.resource('dynamodb')
        tableComments=dynamodb.Table('comments')
        res=tableComments.put_item(
            Item={
                'commentId':id,
                'username':data['username'],
                'age':data['age'],
                'sex':data['sex'],
                'country':data['country'],
                'state':data['state'],
                'comment':data['comment'],
                'sentiment':sentiment,
                'topic':data['topic'],
                'date':str(date_time)
            }
        )
        print('Response from dynamo: {} '.format(res))
        if res['ResponseMetadata']['HTTPStatusCode']!=200:
            statusCode=304
            msg='Error adding comment'
        else:
            statusCode=statusCodeSuccess
            msg=msgSuccess
    except:
        print("[Error 16] problem saving comment in data base")
        raise
    body = {
        "message": msg,
        "id generated": id,
        "data": data
    }
    response = {
        "statusCode": statusCode,
        "body": json.dumps(body)
    }
    return response

# GET ALL COMMENTS
def getComments(event, context):
    #import botocore
    #from boto3.dynamodb.conditions import Key
    dynamodb=boto3.resource('dynamodb')
    tableComments=dynamodb.Table('comments')

    ## ----------------------------------------------------------- 1er método --------------
    '''
    res=tableComments.query(
        KeyConditionExpression=Key('commentId').eq('CI1280838')
    )
    for item in res['Items']:
        item['age']=int(item['age'])
    '''
    ## -------------------------------------------------------------------------------------
    ## ------------------------------------------------------------ 2do método -------------
    ''' Se define un método para manejar la paginación cuando el tmaño de la tabla
        es suficientemente grande, y recorrer todo'''
    scan_kwargs={
        #'FilterExpression':Key().eq()
        'ProjectionExpression':'commentId,username,country,#co',
        'ExpressionAttributeNames':{'#co':'comment'}
    }
    done=False
    start_key=None
    try:
        while not done:
            if start_key:
                scan_kwargs['ExclusiveStartKey']=start_key
            res=tableComments.scan(**scan_kwargs)
            print(res)
            displayData(res.get('Items',[]))
            start_key=res.get('LastEvaluatedKey',None)
            done=start_key is None
    except:
        print('[error 22] problema al obtener registros de comentarios')
        raise
    
    body = {
        "message": "comments loaded successfully! ",
        "comments": res.get('Items',[])
    }
    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }
    return response

# SAVE REPORT FILE IN S3
def generateReport(event, context):
    import pandas as pd

    dynamodb=boto3.resource('dynamodb')
    s3_client=boto3.client('s3')
    tableComments=dynamodb.Table('comments')
    scan_kwargs={
        #'FilterExpression':Key().eq()
        'ProjectionExpression':'commentId,username,country,#co',
        'ExpressionAttributeNames':{'#co':'comment'}
    }
    done=False
    start_key=None
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey']=start_key
        res=tableComments.scan(**scan_kwargs)
        df=addPage(res.get('Items',[]))
        start_key=res.get('LastEvaluatedKey',None)
        done=start_key is None
    print('ok: ',df.columns.values.tolist())
    filename='data_'+str(datetime.now())+'.csv'
    bucket='bucket-hermus'
    key='reports/comments/{}'.format(filename)
    csv_buffer=StringIO()
    df.to_csv(csv_buffer, index=False)
    resS3=s3_client.put_object(
        Body=csv_buffer.getvalue(),
        Bucket=bucket,
        Key=key
    )
    print(resS3)
    body = {
        "message": "data downloaded!, report generated!",
        "S3 object:": resS3
    }
    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }
    return response

# ANALYSZE REPORT IN S3
def analyzeReport(event, context):
    from urllib.parse import unquote_plus
    s3_client=boto3.client('s3')
    for record in event['Records']:
        try:
            bucket=record['s3']['bucket']['name']
            key=unquote_plus(record['s3']['object']['key'])
            print('bucket:{}, key:{}'.format(bucket,key))
            dataObj=s3_client.get_object(Bucket=bucket, Key=key)
            report=parseReport(dataObj)
            csv_buffer=StringIO()
            report.to_csv(csv_buffer)
            filename=key.split('/')[-1]
            res=s3_client.put_object(
                Body=csv_buffer.getvalue(),
                Bucket=bucket,
                Key='reports/comments/report_{}'.format(filename)
            )
        except Exception as err:
            print(err)
        print('Put object successfull!',res)

# DOWNLOAD REPORT GENERATED
def downloadReport(event, context):
    import base64
    filename=event['pathParameters']['name']
    PREFIX='reports/comments/'
    BUCKET_NAME = 'bucket-hermus' # replace with your bucket name
    s3_client=boto3.client('s3')

    if filename=='last':
        print('filename: last...')
        fileList=s3_client.list_objects_v2(
        Bucket=BUCKET_NAME,
        MaxKeys=30,
        Prefix=PREFIX,
        )
        lastFile=fileList['Contents'][-1]['Key'].split('/')[-1]
        print('Last file: ',lastFile)
        filename=lastFile
    else:
        filename=filename.replace('%20',' ')
    
    key = PREFIX+filename # replace with your object key
    print('Key: ',key)
    print('file: ',filename)
    fileObj=s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
    file_content=fileObj['Body'].read()
    response = {
        "statusCode": 200,
        "headers":{
            "Content-Type":"application/csv",
            "Content-Disposition":"attachment; filename={}".format(filename)
        },
        "body": file_content,
        "isBase64Encoded":False
    }
    return response
    '''
    s3 = boto3.resource('s3')
    try:
        s3.Bucket(BUCKET_NAME).download_file(KEY, '/tmp/'+filename)
    except botocore.exceptions.ClientError as e:
        print(e)
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise
    print('archivo descargado!:')
    '''
    '''
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(BUCKET_NAME)
    with open(filename, 'wb') as data:
        bucket.download_fileobj(KEY, data)
    '''

# GET LIST OF REPORTS
def getReports(event, context):
    # build s3 request
    BUCKET_NAME = 'bucket-hermus' # replace with your bucket name
    PREFIX = 'reports/comments/report'
    s3_client=boto3.client('s3')
    res=s3_client.list_objects_v2(
        Bucket=BUCKET_NAME,
        MaxKeys=30,
        Prefix=PREFIX,
    )
    print('s3 response: ',res)

    # get objects and change datetime format to string in the object list
    files=[]
    reports=res['Contents']
    for report in reports:
        item={}
        item['name']=report['Key'].split('/')[-1]
        item['LastModified']=str(report['LastModified'])
        item['Size']=report['Size']
        #item['id']=report['ETag'].split('\"')[1] ## not necessary
        files.append(item)
    print(files)

    # Build response
    body={
        "message":"reports available",
        "files":files
    }
    response = {
        "statusCode": 200,
        "headers":{
            "Content-Type":"application/json"
        },
        "body": json.dumps(body)
    }
    return response

# UPDATE SPECIFIC COMMENT
def updateComment(event, context):
    id=event['pathParameters']['id']
    data=json.loads(event['body'])
    newComment=data['comment']
    updated=str(datetime.now())
    print('id: {} -----\n body: {}'.format(id,data))
    dynamodb=boto3.resource('dynamodb')
    tableComments=dynamodb.Table('comments')
    res=tableComments.update_item(
        Key={
            'commentId':id
            },
        UpdateExpression='SET #com= :c1, updated= :u1',
        ExpressionAttributeValues={
            ":c1":newComment,
            ":u1":updated
        },
        ExpressionAttributeNames={
            "#com":"comment"
        },
        ReturnValues='UPDATED_NEW'
    )
    print(res)
    body = {
        "message":"Actualizacion completada!",
        "response":res['Attributes']
    }
    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response

# DELETE A SPECIFIC COMMENT
def deleteComment(event, context):
    print(event)
    id=event['pathParameters']['id']
    dynamodb=boto3.resource('dynamodb')
    tableComments=dynamodb.Table('comments')
    res=tableComments.delete_item(
        Key={
            'commentId':id
            },
        ReturnValues='ALL_OLD'
    )
    print(res)
    res['Attributes']['age']=int(res['Attributes']['age'])
    body = {
        "message":"Regsitro eliminado! ",
        "response":res['Attributes']
    }
    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response

# DELETE A SPECIFIC REPORT
def deleteReport(event, context):
    filename=event['pathParameters']['name']
    # build s3 request
    BUCKET_NAME = 'bucket-hermus' # replace with your bucket name
    PREFIX = 'reports/comments/'
    filename=filename.replace('%20',' ')
    key=PREFIX+filename

    s3_client=boto3.client('s3')
    res=s3_client.delete_object(
        Bucket=BUCKET_NAME,
        Key=key
    )
    print(res)
    # Build response
    body={
        "message":"file deleted!",
        "files":res
    }
    response = {
        "statusCode": 200,
        "headers":{
            "Content-Type":"application/json"
        },
        "body": json.dumps(body)
    }
    return response


## /////////////////////////////////////////// Métodos auxiliares ////////////////////////////

# ANALIZE SENTIMENT FROM A SPECIFIC COMMENT
def getSentiment(text, lang):
    print('analize comment:')
    client_Comprehend=boto3.client(service_name='comprehend', region_name='us-east-2')
    response=client_Comprehend.detect_sentiment(Text=text, LanguageCode=lang)
    return response['Sentiment']

# SHOW INFO 
def displayData(comments):
    for comment in comments:
        print(f"*\t Comment ID: {comment['commentId']} - by {comment['username']} in {comment['country']}")

# ADD NEW PAGE
def addPage(comments,dataframe=None):
    import pandas as pd
    if dataframe is not None:
        print("dataframe existe! ")
        return dataframe
    else:
        print("no hay dataframe... se procede a su creación")
        list=[]
        for element in comments:
            list.append([element.get('comment'),element.get('username'),element.get('commentId'),element.get('country')])
        #print(list)
        names=[]
        for key in comments[0]:
            names.append(key)
        #print(names)
        df=pd.DataFrame(list,columns=names)
        return df

# GET METRICS FOR A REPORT
def parseReport(dataObj):
    import pandas as pd
    df=pd.read_csv(dataObj['Body'], sep=',')
    report=df.describe()
    return report