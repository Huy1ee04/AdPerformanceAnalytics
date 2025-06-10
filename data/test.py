import pandas as pd

df = pd.read_parquet('/Users/builehuy/AdPerformanceAnalytics/data/batch_0 (3).parquet')
#in 5 dòng đầu tiên
print(df.head())