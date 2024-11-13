import os
import logging
import pandas as pd
import polars as pl
import datetime as dt


class FileManager:
    def __init__(self, base_dir: str,use_polars: bool = False):
        self.base_dir = base_dir
        self.use_polars = use_polars
        # 设置日志配置
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

    def read_excel(self, file_path: str, sheet_name: str = 0) -> pd.DataFrame | dict[str, pd.DataFrame]:
        """读取指定Excel文件的特定sheet"""
        try:
            if not os.path.exists(file_path):
                logging.error(f"文件不存在: {file_path}")
                return pd.DataFrame()
            data = pd.read_excel(file_path, sheet_name=sheet_name)
            # 检查返回的数据类型
            if isinstance(data, pd.DataFrame):
                logging.info(f"成功读取 {file_path} 中的单个 sheet: {sheet_name}")
            elif isinstance(data, dict):
                logging.info(f"成功读取 {file_path} 中的多个 sheet: {list(data.keys())}")
            return data
        except Exception as e:
            logging.error(f"读取文件 {file_path} 中的 sheet: {sheet_name} 失败: {e}")
            return pd.DataFrame() if isinstance(sheet_name, (str, int)) else {}

    def save_to_excel(self, df: pd.DataFrame, filename: str):
        try:
            if df is None:
                logging.error("DataFrame为空，无法保存")
                return
            date_str = dt.datetime.now().strftime("%Y%m%d")
            output_filename = f"{filename}_预处理_{date_str}.xlsx"
            output_path = os.path.join(self.base_dir, output_filename)
            if os.path.exists(output_path):
                logging.warning(f"文件{output_path}已存在，将覆盖")
                os.remove(output_path)
            df.to_excel(output_path, index=False)
            logging.info(f"数据保存成功，路径：{output_path}")
            return output_path
        except Exception as e:
            logging.error(f"数据保存失败: {e}")
            return None

    def save_to_sheet(self, filename: str, **sheets):
        """使用可变参数将多个 DataFrame 保存到一个 Excel 文件中，每个 DataFrame 作为一个 sheet"""
        # 使用 save_to_excel 方法生成文件路径
        output_path = self.save_to_excel(pd.DataFrame(), filename)
        if output_path:
            if not sheets:
                logging.error("未提供任何 DataFrame，无法保存")
                return
            try:
                with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                    for sheet_name, df in sheets.items():
                        if isinstance(df, pd.DataFrame):
                            df.to_excel(writer, sheet_name=sheet_name, index=False)
                        else:
                            logging.warning(f"{sheet_name}不是DataFrame类型，无法保存")
                logging.info(f"多sheet数据保存成功!")
            except Exception as e:
                logging.error(f"多sheet数据保存失败: {e}")
