import boto3
import keyvalue.parsetriples as ParseTripe
import keyvalue.stemmer as Stemmer
import sys, getopt

dynamodbClient = boto3.client('dynamodb')

def put(table, keyword, inx, value):
    res = dynamodbClient.put_item(
        TableName = table,
        Item = {
            'keyword': {
                'S':keyword
            },
            'inx': {
                'N':inx
            },
            'value': {
                'S':value
            }
        }
    )
    return res

def get(table, keyword, inx):
    res = dynamodbClient.get_item(
        TableName = table,
        Key = {
            'keyword':{
                'S':keyword
            },
            'inx':{
                'N':inx
            }
        }
    )
    return res

def createImagesTable(name):
    table = dynamodb.create_table(
        TableName=name,
        KeySchema=[
            {
                'AttributeName': 'keyword',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'inx',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'keyword',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'inx',
                'AttributeType': 'N'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    print('Creating', name, 'table')
    # Wait until the table exists.
    table.meta.client.get_waiter('table_exists').wait(TableName=name)
    print('Created', name, 'table')
    return table

def dynamoStorage(num):
    #GET and/or create Images Table
    try: 
        images = dynamodbClient.describe_table(TableName="images")
    except dynamodbClient.exceptions.ResourceNotFoundException:
        print('Could not found `images` table')
        images = createImagesTable('images')

    print('Images table load')

    #GET and/or create Labels
    try: 
        labels = dynamodbClient.describe_table(TableName="labels")
    except dynamodbClient.exceptions.ResourceNotFoundException:
        print('Could not found `labels` table')
        labels = createImagesTable('labels')

    print('Labels table load')

    #Read Images and Labels from the file
    parseTriplesImages = ParseTripe.ParseTriples("images.ttl")
    parseTriplesLabels = ParseTripe.ParseTriples("labels_en.ttl")

    print('Images file load')
    print('Labels file load')

    images = {}

    #Filter and upload images
    countImages = 1
    #TODO - verify this validation
    num = int(num)
    while(countImages <= num):
        triple = parseTriplesImages.getNext()
        if(triple[1] == 'http://xmlns.com/foaf/0.1/depiction'):
            if(images.get(triple[0]) is None):
                images[triple[0]] = 1
            else:
                images[triple[0]] += 1
            put('images', triple[0], str(images[triple[0]]), triple[2])
            countImages += 1

    print('Images stored in DynamoDB')

    terms = {}

    #Filter and upload labels
    label = parseTriplesLabels.getNext()
    while(label):
        if(label[0] in images and label[1] == 'http://www.w3.org/2000/01/rdf-schema#label'):
            labels = label[2].split()
            for l in labels:
                l = Stemmer.stem(l)
                if(terms.get(l) is None):
                    terms[l] = 1
                else:
                    terms[l] +=1
                put('labels', l, str(terms[l]), label[0])
        label = parseTriplesLabels.getNext()
    
    print('Labels stored in DynamoDB')


def main(argv):
    try:
        opts, args = getopt.getopt(argv, "hn:")
    except:
        print('dynamoStorage.py -n <numberOfImages>')
        sys.exit(2)

    if(len(opts) == 0):
        print('usage: dynamoStorage.py -n <numberOfImages>')
        sys.exit()

    for opt, arg in opts:
        if opt == '-h':
            print('dynamoStorage.py -n <numberOfImages>')
            sys.exit()
        elif opt == '-n':
            dynamoStorage(arg)


if __name__ == "__main__":
    main(sys.argv[1:])