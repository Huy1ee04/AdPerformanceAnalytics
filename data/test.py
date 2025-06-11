import pandas as pd

# Đọc hai file CSV
a = pd.read_json("/Users/builehuy/AdPerformanceAnalytics/data/google_data/raw_data_ga_ga_20240601.txt", lines=True)
b = pd.read_csv("/Users/builehuy/AdPerformanceAnalytics/data/geo_id.csv")

# Lấy tập hợp mã quốc gia đã có trong b.csv
b_codes = set(b["country_code"].dropna().unique())

# Chọn các dòng ở a.csv KHÔNG có mã đó
filtered = a[~a["country_code"].isin(b_codes)]

# In ra màn hình (hoặc ghi ra file mới)
print(filtered.to_csv(index=False))          # in thẳng