from __future__ import print_function

import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import pandas as pd
import time

result_file = './deals.csv'
spread_ids_file = './spread-ids.csv'

# ид таблиц
data = pd.read_csv(spread_ids_file)
spread_ids = list(data['spread_id'])

# нужен новый файл для сбора ид заказов со всех листов
if os.path.exists(result_file):
  os.remove(result_file)
else:
  print("The file does not exist")


# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

def main():
  SAMPLE_RANGE_NAME = 'заказы ГК!D1:E'

  """Shows basic usage of the Sheets API.
  Prints values from a sample spreadsheet.
  """
  creds = None
  # The file token.json stores the user's access and refresh tokens, and is
  # created automatically when the authorization flow completes for the first
  # time.
  if os.path.exists('token.json'):
    creds = Credentials.from_authorized_user_file('token.json', SCOPES)
  
  # If there are no (valid) credentials available, let the user log in.
  if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
      creds.refresh(Request())
    else:
      flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
      creds = flow.run_local_server(port=53872)
    
  # Save the credentials for the next run
  with open('token.json', 'w') as token:
    token.write(creds.to_json())

  try:
    service = build('sheets', 'v4', credentials=creds)
    list_deals_ids = []

    row_count = 0
    
    # Call the Sheets API
    sheet = service.spreadsheets()

    for spread_id in spread_ids:      
      if row_count > 0:
        SAMPLE_RANGE_NAME = 'заказы ГК!D2:E'

      result = sheet.values().get(spreadsheetId=spread_id,
                                  range=SAMPLE_RANGE_NAME).execute()
      values = result.get('values', [])
      
      #values[index_arr].append(spread_id);

      print('id spread:', spread_id)
      row_count += 1

      if not values:
        print('No data found.')
        continue
    
      for row in values:
        row.append(spread_id)
        if (row[0] not in list_deals_ids):
          list_deals_ids.append(row[0])
        else:
          print('-- found duplicate deal:', row[0])

        # Print columns A and E, which correspond to indices 0 and 4.
        #print('%s' % (row[0]))
      
      pd.DataFrame(values).to_csv(result_file, index=None, header=None, mode = 'a')
      
      time.sleep(1)

    print('processed spreads:', row_count)
  except HttpError as err:
    print(err)


if __name__ == '__main__':
  main()