import pandas as pd

# Đường dẫn tới file CSV cần sửa
file_path = '/Users/builehuy/AdPerformanceAnalytics/data/geo_id.csv'

# Đọc file CSV
df = pd.read_csv(file_path)

# Đổi tên cột
df.rename(columns={
    'Criteria ID': 'country_code',
    'Country Code': 'Country'
}, inplace=True)

# Ghi đè file cũ
df.to_csv(file_path, index=False)