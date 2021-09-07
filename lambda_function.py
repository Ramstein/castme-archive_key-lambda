import json
from csv import DictReader
from datetime import datetime, timezone

import boto3

from fuzzywuzzy.process import extractOne

region = 'ap-south-1'
process_question_answer_table = 'process_question_answer_table'


def lambda_handler(event, context):
    global row, csv_filename
    test = event['queryStringParameters']['test']
    sub_code = str(event['queryStringParameters']['sub_code']).upper()
    question = str(event['queryStringParameters']['question']).upper()
    launch_time = datetime.now(tz=timezone.utc).strftime('%y-%m-%d %H:%M:%S')
    response = {}
    if sub_code == "":
        response = {'sub_code': "", 'question': "", 'answer': ""}
    else:
        csv_filename = sub_code + '.csv'

    if test == "0":  # no test
        if question == "":
            response = {'sub_code': sub_code, 'question': "", 'answer': ""}
        else:
            # with open('names.csv', 'w', newline='') as csvfile:
            #     writer = csv.DictWriter(csvfile, fieldnames=['first_name', 'last_name'])
            #     writer.writeheader()
            #     writer.writerow({'first_name': 'Baked', 'last_name': 'Beans'})

            question1_dict, question2_dict, answer_dict = {}, {}, {}

            with open(csv_filename, newline='') as csvfile:  # os.getcwd() == '/var/task'
                reader = DictReader(csvfile)
            for i, row in enumerate(reader):
                question1_dict.update({i: row['question1']})
                question2_dict.update({i: row['question2']})
                answer_dict.update({i: row['answer']})

            question1_score = extractOne(question, list(question1_dict))
            question2_score = extractOne(question, list(question2_dict))

            dynamodb_cli = boto3.client('dynamodb')

            if question1_score[1] > question2_score[1]:
                for i, question in enumerate(list(question1_dict)):
                    if question == question1_score[0]:
                        if answer_dict[i] == "":
                            response = {'launch_time': launch_time, 'sub_code': sub_code, 'question': question,
                                        'answer': '', 'score': question1_score[1]}
                        else:
                            item = {
                                'launch_time': {'S': str(launch_time)},
                                'sub_code': {'S': sub_code},
                                'question': {'S': question},
                                'answer': {'S': answer_dict[i]},
                                'score': {'S', str(question1_score[1])}
                            }
                            res = dynamodb_cli.put_item(TableName=process_question_answer_table, Item=item)
                            response = {'launch_time': launch_time,
                                        'sub_code': sub_code,
                                        'question': question,
                                        'answer': answer_dict[i],
                                        'score': question1_score[1]
                                        }
                        break
            else:
                for i, question in enumerate(list(question2_dict)):
                    if question == question2_score[0]:
                        if answer_dict[i] == "":
                            response = {'launch_time': launch_time, 'sub_code': sub_code, 'question': question,
                                        'answer': '', 'score': question2_score[1]}
                        else:
                            item = {
                                'launch_time': {'S': str(launch_time)},
                                'sub_code': {'S': sub_code},
                                'question': {'S': question},
                                'answer': {'S': answer_dict[i]},
                                'score': {'S', str(question2_score[1])}
                            }
                            res = dynamodb_cli.put_item(TableName=process_question_answer_table, Item=item)
                            response = {'launch_time': launch_time,
                                        'sub_code': sub_code,
                                        'question': question,
                                        'answer': answer_dict[i],
                                        'score': question2_score[1]
                                        }
                        break
    else:
        if sub_code != "":
            try:
                lines = open(csv_filename, 'r').readlines()
                if lines is None:
                    response = {'launch_time': launch_time, 'sub_code': sub_code, 'question': question, 'answer': '',
                                'score': 0}
            except FileNotFoundError:
                response = {'launch_time': launch_time, 'sub_code': '', 'question': '', 'answer': '', 'score': 0}
        else:
            response = {'launch_time': launch_time, 'sub_code': '', 'question': '', 'answer': '', 'score': 0}
    print(response)
    return {
        'statusCode': 200,
        'headers': {'Content-type': 'application/json'},
        'body': json.dumps(response)
    }
