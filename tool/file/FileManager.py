import os
import logging
import polars as pl
import datetime as dt
from tqdm import tqdm
from openpyxl import Workbook, load_workbook


class FileManager:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        # 设置日志配置
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def _save_dataframe_to_sheet(self, worksheet, df: pl.DataFrame):
        """封装写入 Polars DataFrame 到 Excel sheet 的方法"""
        # 将 Polars DataFrame 转为字典列表
        data = df.to_dicts()

        # 获取列名
        columns = df.columns
        # 写入列名（header）
        for col_num, col_name in enumerate(columns, start=1):
            worksheet.cell(row=1, column=col_num, value=col_name)

        # 写入数据
        for row_num, row in enumerate(data, start=2):
            for col_num, col_name in enumerate(columns, start=1):
                worksheet.cell(row=row_num, column=col_num, value=row[col_name])

        logging.info(f"已保存 sheet: {worksheet.title}")

    def _generate_output_path(self, filename: str) -> str:
        """生成输出文件的路径，并检查文件是否存在"""
        try:
            date_str = dt.datetime.now().strftime("%Y%m%d")
            output_filename = f"{filename}_预处理_{date_str}.xlsx"
            output_path = os.path.join(self.base_dir, output_filename)

            if os.path.exists(output_path):
                logging.warning(f"文件 {output_path} 已存在，将覆盖")
                os.remove(output_path)

            return output_path
        except Exception as e:
            logging.error(f"生成文件路径失败: {e}")
            return None

    def get_latest_file(self, dir_name: str, file_extension=(".xlsx", ".xls")) -> str:
        source_dir = os.path.join(self.base_dir, dir_name)
        if not os.path.exists(source_dir):
            logging.error(f"未找到指定目录: {source_dir}")
            exit(1)

        try:
            files = [f for f in os.listdir(source_dir)
                     if os.path.isfile(os.path.join(source_dir, f)) and f.endswith(file_extension)]
            if not files:
                logging.warning(f"目录 {source_dir} 中不存在文件")
                return ""

            latest_file = max(files, key=lambda x: os.path.getctime(os.path.join(source_dir, x)))
            return os.path.join(source_dir, latest_file)
        except Exception as e:
            logging.error(f"获取最新文件时发生错误: {e}")
            return ""

    def read_excel(self, file_path: str, sheet_name: str = 0) -> pl.DataFrame | dict[str, pl.DataFrame]:
        """读取指定Excel文件的特定sheet"""
        try:
            if not os.path.exists(file_path):
                logging.error(f"文件不存在: {file_path}")
                return pl.DataFrame()
            data = pl.read_excel(file_path, sheet_name=sheet_name)
            # 检查返回的数据类型
            if isinstance(data, pl.DataFrame):
                logging.info(f"成功读取 {file_path} 中的单个 sheet: {sheet_name}")
            elif isinstance(data, dict):
                logging.info(f"成功读取 {file_path} 中的多个 sheet: {list(data.keys())}")
            return data
        except Exception as e:
            logging.error(f"读取文件 {file_path} 中的 sheet: {sheet_name} 失败: {e}")
            return pl.DataFrame() if isinstance(sheet_name, (str, int)) else {}

    def save_to_excel(self, df: pl.DataFrame, filename: str):
        try:
            if df is None:
                logging.error("DataFrame为空，无法保存")
                return
            output_path = self._generate_output_path(filename)
            if os.path.exists(output_path):
                logging.warning(f"文件{output_path}已存在，将覆盖")
                os.remove(output_path)
            # 创建一个新的工作簿
            workbook = Workbook()
            worksheet = workbook.active
            
            # 使用封装的方法保存 DataFrame
            self._save_dataframe_to_sheet(worksheet, df)

            # 保存 Excel 文件
            workbook.save(output_path)
            logging.info(f"数据保存成功，路径：{output_path}")

            logging.info(f"数据保存成功，路径：{output_path}")
            return output_path
        except Exception as e:
            logging.error(f"数据保存失败: {e}")
            return None

    def save_to_sheet(self, filename: str, **sheets):
        
        """将多个 Polars DataFrame 保存到一个 Excel 文件的多个 sheet"""
        try:
            output_path = self._generate_output_path(filename)
 
            workbook = Workbook()
            # 默认创建一个空的Sheet，删除它
            if 'Sheet' in workbook.sheetnames:
                del workbook['Sheet']
            # 遍历每个 DataFrame，写入对应的 sheet
            for sheet_name, df in tqdm(sheets.items(), desc="正在保存 Sheets", unit="sheet"):
                if isinstance(df, pl.DataFrame):
                    # 创建一个新的 sheet
                    worksheet = workbook.create_sheet(sheet_name)
                    # 使用封装的方法保存 DataFrame
                    self._save_dataframe_to_sheet(worksheet, df)
                else:
                    logging.warning(f"{sheet_name} 不是 Polars DataFrame 类型，无法保存")

            # 保存 Excel 文件
            workbook.save(output_path)
            logging.info(f"多sheet数据保存成功，路径：{output_path}")

        except Exception as e:
            logging.error(f"多sheet数据保存失败: {e}")
