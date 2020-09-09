from __future__ import print_function
import pickle
import os.path
import csv
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

def main():
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    spreadsheet_id = '1VbZs3393JGKkdwl2LHRTXuRJoqTXcE16AO-mrkT2vw0'
    range_name = 'Sheet1!A:B'
    #values = [[ 15, 12, 55 ], [ 14, 55, 44 ], [ "yo", "ya" ]]
    with open('file.csv', 'r') as f:
      reader = csv.reader(f)
      values = list(reader)
    value_input_option = 'RAW'
    #values = _values
    creds = None
    body = {
        'values': values
        }
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
            
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', scope)
            creds = flow.run_local_server()
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    
    service = build('sheets', 'v4', credentials=creds)

    result = service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id, range=range_name,
            valueInputOption=value_input_option, body=body).execute()
    """print('{0} cells appended.'.format(result \
                                       .get('updates') \
                                       .get('updatedCells')))"""
    return result
    

if __name__ == '__main__':
    main()
