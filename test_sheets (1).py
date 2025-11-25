import os
import traceback
import gspread
from oauth2client.service_account import ServiceAccountCredentials

SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID') or '1ZBIQxkDocI60v68SU3x8PVW--0ahNoOxOyKaE60TtwY'
CREDENTIALS_FILE = os.environ.get('GOOGLE_CREDENTIALS_JSON') or 'credentials.json'

print('SPREADSHEET_ID =', SPREADSHEET_ID)
print('CREDENTIALS_FILE =', CREDENTIALS_FILE)

try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    client = gspread.authorize(creds)
    sh = client.open_by_key(SPREADSHEET_ID)
    sheet = sh.sheet1
    print('Connected. Sheet title:', sh.title)
    # Try to append a test row
    from datetime import datetime
    row = [datetime.now().isoformat(), 'test', 'тестовый пользователь', 'тестовый текст']
    sheet.append_row(row, value_input_option='USER_ENTERED')
    print('Test row appended successfully.')
except Exception as e:
    print('Failed to connect or write to Google Sheets:')
    traceback.print_exc()
    print('\nПроверьте:')
    print('- Путь к credentials.json правильный и файл существует')
    print('- Таблица шарена с client_email из credentials.json (проверьте поле client_email)')
    print('- SPREADSHEET_ID указан правильно (только ID, не URL)')
    print('- У вас есть доступ в интернет и нет блокировки Google API')
