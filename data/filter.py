import pandas as pd

# Đọc file JSON lines vào DataFrame
df = pd.read_json('/Users/builehuy/AdPerformanceAnalytics/data/facebook_data/raw_data_fb_fb_20240609.txt', lines=True)

# Lọc các dòng có "Impr." bị null (NaN)
null_impr_df = df[df['Impressions'].isna()]

# In kết quả
print(null_impr_df)
