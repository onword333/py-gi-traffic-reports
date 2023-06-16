import pandas as pd
import glob
import os

REGS_PATH = 'regs_data_orig/'
REGS_PATH_OUT = 'regs_data_out/'

colunm_types = {
  'Дата создания': 'string',
  'ID пользователя': 'string',
  'utm_source': 'string',
  'utm_medium': 'string',
  'utm_campaign': 'string',
  'utm_content': 'string',
  'utm_term': 'string'
}

count_file = 0

all_files = glob.glob(os.path.join(REGS_PATH, '*.csv'))

for filename in all_files:  
  orig_filename = os.path.basename(filename)  
  data = pd.read_csv(filename, sep = ';', dtype = colunm_types, keep_default_na=False); #keep_default_na=False
  #print(data.info(memory_usage='deep'))

  data.rename(columns = {'Дата создания': 'date_reg', 'ID пользователя': 'gk_id'}, inplace = True)
  data = data[['date_reg', 'gk_id', 'utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term']]
  
  # если у пользователя несколько регистраций в воронку, то возьмем первую
  # отсортируем по дате, уберем время
  data_groupby_first = data.groupby(['gk_id'], as_index=False).first()
  data_groupby_first.sort_values(by = 'date_reg', inplace = True)
  data_groupby_first['date_reg'] = data_groupby_first['date_reg'].str.replace(' [0-9].+', '', regex = True)
  data_groupby_first.to_csv(REGS_PATH_OUT + 'processed_' + orig_filename, index  = False)
  print('processe', filename)
  count_file += 1

print('files processed:', count_file)
