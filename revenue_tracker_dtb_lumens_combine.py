import pandas as pd
from google.cloud import bigquery
import os
import datetime
import os
import tempfile
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
from datetime import datetime

#download from One Drive
def downloadFromOneDrive(file_path, file_url):
    with open(file_path, "wb") as local_file:
        file = (
            ctx.web.get_file_by_server_relative_url(file_url)
            .download(local_file)
            .execute_query()
        )
    print("\033[32m[Ok] file has been downloaded into: {0}\033[0m".format(file_path))

# 设置Google Cloud凭据
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "C:/Users/zxy25/OneDrive/Desktop/service-account-key.json"

#One Drive user name and password
username = "XIAOYU.ZENG@lumens.sg"
pw = "Sft8253h!"

#Colleague's one drive path
url = "https://lumensautopl-my.sharepoint.com/personal/jiawen_lee_lumens_sg/"

file_urls = [ "Documents/Finance%20%2D%20External/DTB/2410 October Daily Transaction Book - Lumens.xlsx", # file to download
              "Documents/Finance%20%2D%20External/DTB/2411 November Daily Transaction Book - Lumens.xlsx" #to add in more files
            ]
sheet_name = "Billing Record (CRM)"  # 指定工作表的名称

ctx = ClientContext(f'{url}').with_credentials(UserCredential(f"{username}", f"{pw}"))
# timestamp = datetime.now().strtime("%Y-%m-%d")
df_list = []
prefix = ""

for i in range(len(file_urls)):

    file_path = prefix + file_urls[i].split('/')[-1]
    downloadFromOneDrive(file_path, file_urls[i])
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=2)
    print(f"\033[32m[Ok] {file_path} loaded:\033[0m")
    print(df.head())  # 打印数据的前几行
    # 获取前16列
    df = df.iloc[:, :16]
    # 清理列名：将列名转换为小写，替换空格为下划线，并删除/替换不支持的字符
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace(r'[^\w]', '', regex=True)
    # 清理数据中的特殊符号，例如删除类似 "driver's" 中的单引号
    df = df.replace(r"[^\w\s]", '', regex=True)
    # 将 'billing_date' 列转换为日期格式
    df['billing_date'] = pd.to_datetime(df['billing_date'], errors='coerce')  # 如果有无效日期，转换为NaT
    # 将日期格式化为 'YYYY-MM-DD'
    df['billing_date'] = df['billing_date'].dt.strftime('%Y-%m-%d')
    df_list.append(df)

# 合并两个DataFrame
combined_df = pd.concat(df_list, ignore_index=True)

# 保存合并后的数据为CSV文件
csv_file_path = prefix + 'lumens_combined_data.csv'
combined_df.to_csv(csv_file_path, index=False)

print("\033[32m[Ok] Data successfully combined and saved.\033[0m")

# BigQuery client setup
client = bigquery.Client()

# 设置目标数据集和表名
dataset_id = 'auto_data_ingest'
table_id = 'billing_record_crm'

# 设置表的引用
table_ref = client.dataset(dataset_id).table(table_id)

# 检查表是否存在，如果存在则删除该表
try:
    client.delete_table(table_ref)  # 如果表存在则删除
    print(f"Deleted table {dataset_id}.{table_id}.")
except Exception as e:
    print(f"\033[31m[Error] Table {dataset_id}.{table_id} does not exist or could not be deleted. Error: {e}\033[0m")

# 配置加载作业
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,  # 跳过CSV文件的标题行
    autodetect=True       # 自动检测表的模式
)

# 打开CSV文件并上传到BigQuery
with open(csv_file_path, 'rb') as csv_file:
    load_job = client.load_table_from_file(csv_file, table_ref, job_config=job_config)

# 等待加载作业完成
load_job.result()

# 获取表以验证加载
destination_table = client.get_table(table_ref)
print(f"\033[32m[Ok] Loaded {destination_table.num_rows} rows into {dataset_id}.{table_id}.\033[0m")
