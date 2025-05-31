import pandas as pd

# Đường dẫn đến file CSV gốc
#input_file = '/Users/builehuy/AdPerformanceAnalytics/geotargets-2025-04-01.csv'  

# Đường dẫn để lưu file CSV mới
output_file = '/Users/builehuy/AdPerformanceAnalytics/data/geo_id.csv'

# Dòng bắt đầu và kết thúc (theo chỉ số dòng trong file CSV, dòng tiêu đề là dòng 1)
start_line = 29565
end_line = 31045

# Số dòng cần đọc
num_rows = end_line - start_line + 1

# Đọc file với skiprows và nrows
df = pd.read_csv(input_file, skiprows=range(1, start_line), nrows=num_rows)

# Đọc lại header từ dòng đầu tiên để giữ tên cột
header = pd.read_csv(input_file, nrows=0)
df.columns = header.columns


# Lọc các bản ghi theo điều kiện:
filtered_df = df[
    ((df['Target Type'] == 'Country') | (df['Target Type'] == 'Region')) &
    (df['Parent ID'].isna())
]

# Chỉ giữ lại 2 cột mong muốn
final_df = filtered_df[['Criteria ID', 'Country Code']]

# Lưu dữ liệu trích xuất ra file mới
final_df.to_csv(output_file, index=False)

