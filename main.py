import boto3

dynamodb = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')
TABLE_NAME = 'Pessoas'
TABLE = dynamodb.Table(TABLE_NAME)

def createTable():
    existing_tables = dynamodb_client.list_tables()['TableNames']
    if TABLE_NAME not in existing_tables:
        table = dynamodb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {
                    'AttributeName': 'cpf',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'cpf',
                    'AttributeType': 'N'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        # Wait until the table exists.
        table.meta.client.get_waiter('table_exists').wait(TableName='Pessoa2')
        print('Table crated')
    else:
        print('Table already exists')


def deleteTable():
    TABLE.delete()
    print('Table deleted')


def insertPessoa(values):
    TABLE.put_item(
        Item={
            'cpf': values['cpf'],
            'nome': values['nome'],
            'idade': values['idade'],
        }
    )
    print('Inserted person: {}'.format(getPessoa(values['cpf'])))


def getPessoa(cpf):
    response = TABLE.get_item(
        Key={
            'cpf': cpf,
        }
    )
    return response['Item']


def deletePessoa(cpf):
    TABLE.delete_item(
        Key={
            'cpf': cpf,
        }
    )
    print('Person with cpf equals {} deleted'.format(cpf))


def updatePessoa(values):
    try:
        TABLE.update_item(
            Key={
                'cpf': values['cpf']
            },
            UpdateExpression='SET nome = :nome, idade = :idade',
            ExpressionAttributeValues={
                ':nome': values['nome'],
                ':idade': values['idade']
            }
        )
        print('Person: {} added'.format(getPessoa(values['cpf'])))
    except Exception as e:
        print(e)


if __name__ == '__main__':
    createTable()
    insertPessoa({'cpf': 222, 'nome': 'Arthur', 'idade': 20})
    print('Result of CPF 222 search:: {}'.format(getPessoa(222)))
    updatePessoa({'cpf': 222, 'nome': 'Arthur Carminati', 'idade': 21})
    deletePessoa(222)
    deleteTable()
