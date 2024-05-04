import json
import boto3
import datetime
from botocore.exceptions import ClientError
import pandas as pd

# Handler function for AWS Lambda
def lambda_handler(event, context):
     
    # Initialize the AWS Cost Explorer client
    billing_client = boto3.client('ce')
    
    # Get today's date and the date one week ago
    today = datetime.date.today()
    str_today = str(today)  # Convert today's date to string
    week = today - datetime.timedelta(days=7)  # Calculate the date 7 days ago
    str_week = str(week)  # Convert the week-ago date to string
     
    # Request cost and usage data for the last week
    response_total = billing_client.get_cost_and_usage(
       TimePeriod={
         'Start': str_week,
         'End': str_today
       },
       Granularity='MONTHLY',  # Specifies the aggregation level
       Metrics=['UnblendedCost']  # Type of cost metric to retrieve
    )
     
    # Print the response data from Cost Explorer
    print(response_total)
    
    # Check how many time segments are returned in the response
    length = len(response_total["ResultsByTime"])
    print(length)
     
    # If two monthly segments are included in the results
    if length == 2:
        # Extract and sum the costs from both segments
        total_cost_1 = float(response_total["ResultsByTime"][0]['Total']['UnblendedCost']['Amount'])
        total_cost_2 = float(response_total["ResultsByTime"][1]['Total']['UnblendedCost']['Amount'])
        total_cost = round(total_cost_1 + total_cost_2, 3)  # Sum and round the total cost
        total_cost = '$' + str(total_cost)  # Format the total cost as a string with a dollar sign
        
        print('Total cost for the week: ' + total_cost)
        
        # Request detailed cost and usage data, grouped by service and usage type
        response_detail = billing_client.get_cost_and_usage(
            TimePeriod={
                'Start': str_week,
                'End': str_today
            },
            Granularity='MONTHLY',
            Metrics=['UnblendedCost'],
            GroupBy=[
                {'Type': 'DIMENSION', 'Key': 'SERVICE'},
                {'Type': 'DIMENSION', 'Key': 'USAGE_TYPE'}
            ]
        )
         
        # Initialize dictionaries to store cost data for aggregation
        resources = {'Service':[], 'Usage Type':[], 'Cost':[]}
        resources_1 = {'Service':[], 'Usage Type':[], 'Cost':[]}
         
        # Process the first segment of detailed cost data
        for result in response_detail['ResultsByTime'][0]['Groups']:
            service, usage_type, cost = result['Keys'][0], result['Keys'][1], result['Metrics']['UnblendedCost']['Amount']
            if float(cost) > 0:  # Filter out zero costs
                resources['Service'].append(service)
                resources['Usage Type'].append(usage_type)
                resources['Cost'].append('$' + str(round(float(cost), 3)))
                 
        # Process the second segment of detailed cost data
        for result in response_detail['ResultsByTime'][1]['Groups']:
            service, usage_type, cost = result['Keys'][0], result['Keys'][1], result['Metrics']['UnblendedCost']['Amount']
            if float(cost) > 0:  # Filter out zero costs
                resources_1['Service'].append(service)
                resources_1['Usage Type'].append(usage_type)
                resources_1['Cost'].append('$' + str(round(float(cost), 3)))
                 
        # Combine data from both time segments
        for key, value in resources_1.items():
            resources[key].extend(value)

    # Process the case with only one time segment in the results
    else:
        total_cost = float(response_total["ResultsByTime"][0]['Total']['UnblendedCost']['Amount'])
        total_cost = '$' + str(round(total_cost, 3))
        print('Total cost for the week: ' + total_cost)
        
        # Repeat the detailed data request and processing as above
        response_detail = billing_client.get_cost_and_usage(
            TimePeriod={
                'Start': str_week,
                'End': str_today
            },
            Granularity='MONTHLY',
            Metrics=['UnblendedCost'],
            GroupBy=[
                {'Type': 'DIMENSION', 'Key': 'SERVICE'},
                {'Type': 'DIMENSION', 'Key': 'USAGE_TYPE'}
            ]
        )
         
        resources = {'Service':[], 'Usage Type':[], 'Cost':[]}
         
        for result in response_detail['ResultsByTime'][0]['Groups']:
            service, usage_type, cost = result['Keys'][0], result['Keys'][1], result['Metrics']['UnblendedCost']['Amount']
            if float(cost) > 0:
                resources['Service'].append(service)
                resources['Usage Type'].append(usage_type)
                resources['Cost'].append('$' + str(round(float(cost), 3)))
                 
    print(type(resources))
             
    # Convert the collected data into a Pandas DataFrame and then to an HTML table
    df = pd.DataFrame(resources)
    html_table = df.to_html(index=False)
             
    print(resources)
     
    # Compose an HTML message for the email body
    message = 'Cost of AWS PROD account for the past week was'
     
    html = """
            <html>
              <head>
                <style>
                  body {{
                    font-family: Arial, sans-serif;
                    color: black;
                    background-color: white;
                    text-align: center;
                  }}
                  h2 {{
                    color: white;
                    font-size: 25px;
                    text-align: center;
                  }}
                  h1 {{
                    color: white;
                    font-size: 40px;
                    text-align: center;
                    background-color: orange;
                  }}
                  p {{
                    color: black;
                    font-size: 30px;
                    line-height: 1.5;
                    margin-bottom: 20px;
                    text-align: center;
                  }}
                  p1 {{
                     font-size: 10px;
                     text-align: center;
                  }}
                </style>
              </head>
              <body>
                <p> PROD account report for the week {} and {} </p>
                <h2> {} </h2>
                <h1> <strong> <em> {} </em></strong> </h1>
                <p1>{}</p1>
              </body>
            </html>
            """.format(str_week, str_today, message, total_cost, html_table)
                  
    # Initialize the Simple Email Service (SES) client
    ses_client = boto3.client('ses', region_name='us-west-2')
     
    # Set up the email content and recipient details
    message = {
        'Subject': {'Data': 'AWS PROD account cost report'},
        'Body': {'Html': {'Data': html}}
    }
    # Replace emails with yours, sender should be authorized in SES.
    response = ses_client.send_email(
        Source='sender@test.com',
        Destination={'ToAddresses': ['example1@test.com','example2@test.com']},
        Message=message
    )
     
    # Print the SES response for verification
    print(response)
