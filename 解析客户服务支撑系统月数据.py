import logging
import glob
import os.path

from tqdm import tqdm  # 导入tqdm
import polars as pl
from openpyxl.reader.excel import load_workbook
from tool.file import FileManager

def process_excel(excel_data_df: pl.DataFrame):
    # 选择所需的列
    columns_to_keep = ['客服流水号', '受理号码', '区域', '系统接单时间', '月份']
    excel_data_df = excel_data_df.select(columns_to_keep)
    return excel_data_df


def standardize_column_types(df: pl.DataFrame) -> pl.DataFrame:
    # 在这里进行列类型统一的转换，假设我们希望所有列都转换为字符串类型
    for col in df.columns:
        # 假设将所有列类型统一为字符串，如果有其他需求，可以根据实际情况进行调整
        df = df.with_columns(pl.col(col).cast(pl.Utf8))
    return df


def process_dataframe(main_dataframe:pl.DataFrame):
    main_dataframe = main_dataframe.with_columns((pl.col("区域")+"-"+pl.col("受理号码")).alias("区域-受理号码"))
    
    for month in range(1, 11):
        main_dataframe = main_dataframe.with_columns(pl.lit(None).cast(pl.Utf8).alias(f"{month}月"))
    
    
    main_dataframe = main_dataframe.unique(subset=["客服流水号", "区域-受理号码", "月份"], keep="first")

    
    # 按“区域-受理号码”和“月份”分组，统计每个组合在该月的重复次数
    grouped = main_dataframe.group_by(["区域-受理号码", "月份"]).agg([
        pl.len().alias("重复次数")
    ])

    # 提取月份的数字部分，假设月份格式为 "YYYYMM"
    main_dataframe = main_dataframe.with_columns(
        pl.col("月份").str.slice(4, 2).cast(pl.Int32).alias("月份数字")
    )
    
    # 将“重复次数”添加回原数据框
    main_dataframe = main_dataframe.join(grouped, on=["区域-受理号码", "月份"], how="left")


    # main_dataframe = main_dataframe.unique(subset="客服流水号")
    main_dataframe = main_dataframe.unique(subset=["区域-受理号码", "月份"], keep="first")

    # 填充每一行对应月份的投诉次数
    for month in range(1, 11):
        # 检查当前行的“月份”，如果匹配，则填充“重复次数”到对应的月份列
        main_dataframe = main_dataframe.with_columns(
            pl.when(pl.col("月份数字").cast(pl.Utf8) == str(month))  # 如果月份匹配
            .then(pl.col("重复次数"))  # 填充重复次数
            .otherwise(pl.lit(None))  # 否则填充 None
            .alias(f"{month}月")  # 对应月份的列
        )

        # 按照“月份数字”排序，确保数据按月份顺序排列
    main_dataframe = main_dataframe.sort("月份数字")

    # 删除“重复次数”列，因为已经填充到月份列中了
    main_dataframe = main_dataframe.drop(["月份数字","重复次数"])
    
    return main_dataframe

if __name__ == '__main__':
    try:
        file_manager = FileManager("WorkDocument")
        file_list = file_manager.get_list_files("202401-10月支撑系统")

        main_dataframe = pl.DataFrame()
    
        for file in tqdm(file_list, desc="读取文件列表中"):
            data_df = file_manager.read_excel(file)
            # 确保列类型一致
            data_df = standardize_column_types(data_df)
            data_df = process_excel(data_df)
            main_dataframe = pl.concat([main_dataframe, data_df],how="vertical")
            
        main_dataframe = process_dataframe(main_dataframe)
                    
        file_manager.save_to_excel(main_dataframe, "全月份投诉明细.xlsx")
        
    except Exception as e:
        logging.error(e)
