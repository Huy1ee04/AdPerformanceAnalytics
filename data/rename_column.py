import pandas as pd

# Đường dẫn tới file CSV cần sửa
file_path = '/Users/builehuy/AdPerformanceAnalytics/data/geo_id_2.csv'

# Đọc file CSV
df = pd.read_csv(file_path)
final_df = df[['Country Code', 'Name']]
# Đổi tên cột
final_df.rename(columns={
    'Country Code': 'country',
    'Name': 'country_name'
}, inplace=True)

# Ghi đè file cũ
final_df.to_csv(file_path, index=False)