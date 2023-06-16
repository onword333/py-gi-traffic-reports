from __future__ import print_function

import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import pandas as pd
import time

spread_ids_file = './spread-ids.csv'
all_data_file_name = './all-data.csv'

# ид таблиц
data = pd.read_csv(spread_ids_file)
spread_ids = list(data['spread_id'])


# нужен новый файл для всех загруженных данных
if os.path.exists(all_data_file_name):
  os.remove(all_data_file_name)
else:
  print("The file does not exist")

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']


def main():
  SAMPLE_RANGE_NAME = 'реги!A1:W'

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


    row_count = 0

    # Call the Sheets API
    sheet = service.spreadsheets()

    for spread_id in spread_ids:      
      if row_count > 0:
        SAMPLE_RANGE_NAME = 'реги!A2:W'

      result = sheet.values().get(spreadsheetId=spread_id,
                                  range=SAMPLE_RANGE_NAME).execute()
      values = result.get('values', [])

      print('Download spread:', spread_id)
      row_count += 1

      if not values:
        print('No data found.')
        continue

      # for row in values:
      #   # Print columns A and E, which correspond to indices 0 and 4.
      #   print('%s' % (row[0]))
      
      pd.DataFrame(values).to_csv(all_data_file_name, index=None, header=None, mode = 'a')      
      time.sleep(2)

    print('processed spreads:', row_count)
    
  except HttpError as err:
    print(err)

  data_types = {
    'date_reg': 'string',
    'имя воронки': 'string',
    'имя подрядчика': 'string',
    'канал': 'string',
    'source / medium / campaign / content / term': 'string',
    'имя группы': 'string',
    'описание группы': 'string',
    'fun': 'string',
    'Реги': 'int32', 
    'явки': 'int32', 
    'счета': 'int32', 
    'оплаты': 'int32',
    'доход': 'float32'    
  }

  # группируем полученные данные и суммируем значения
  data = pd.read_csv(all_data_file_name, sep= ',', dtype = data_types, keep_default_na=False); #keep_default_na=False

  # если в рег. настройка файла десятичный разделитель определено как ",",
  # то делаем замену на точку "."
  data['расход'] = data['расход'].str.replace(',', '.')
  data['расход'] = data['расход'].apply(lambda x: x.strip()).replace('', 0)

  data = data.astype({'расход': 'float32'})

  data = data.drop(columns=[
    'utm_source', 
    'utm_medium',
    'utm_campaign',
    'utm_content',
    'utm_term',    
    'воронка / продукт',
    'подрядчик',
    'source / medium',
    'gk_id'
  ])

  print(data.info(memory_usage='deep'))


  index = [
    'date_reg', 
    # 'utm_source', 
    # 'utm_medium', 
    # 'utm_campaign', 
    # 'utm_content', 
    # 'utm_term',
    'имя воронки',
    'имя подрядчика',
    'канал',
    'source / medium / campaign / content / term',
    'имя группы',
    'описание группы',
    'fun'
  ];

  #values = ['Реги', 'явки', 'счета', 'оплаты', 'доход', 'расход']

  group_table = data.groupby(index).sum()
  group_table.to_csv('group_table.csv')


if __name__ == '__main__':
  main()