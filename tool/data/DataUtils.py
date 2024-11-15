import re
from typing import List, Union
import polars as pl
import datetime as dt
import logging

class DataUtils:

    def __init__(self, dataframe: pl.DataFrame):
        self.dataframe = dataframe

    def insert_colum(self, left_col, right_col, offset: int = 1) -> pl.DataFrame:
        """
            插入right_col到left_col之后，偏移量可以指定插入位置的偏移。

            :param left_col: 目标列名，插入位置参考此列。
            :param right_col: 要插入的列名。
            :param offset: 偏移量，默认为1，表示插入到目标列后面；如果为0，则插入到目标列前面。
            :return: 新的DataFrame，包含插入的列。
            """
        
        idx = self.dataframe.columns.index(left_col) + offset  # 获取“系统接单时间”列的索引位置
        return self.dataframe.select(
            self.dataframe.drop(right_col).insert_column(idx, self.dataframe.get_column(right_col))
        )
     
    def filter_data_range(self,date_column:str,start_time:dt.datetime = None,end_time:dt.datetime = None,days:int =None)->pl.DataFrame:
        ### 按照日期范围筛选数据 ###
        try:
            df = self.dataframe

            logging.info(f"开始日期过滤，列名: {date_column}")
            logging.info(f"列类型: {df[date_column].dtype}")

            # 如果是字符串类型，尝试转换为日期时间
            if df[date_column].dtype == pl.Utf8:
                logging.info("检测到字符串类型，尝试转换为日期时间")
                df = df.with_columns([
                    pl.col(date_column).str.strptime(
                        pl.Datetime, 
                        format="%Y-%m-%d %H:%M:%S",
                        strict=False
                    ).alias(date_column)
                ])
                logging.info("日期转换成功")

              # 处理null值
            df = df.with_columns([
                pl.col(col).fill_null(strategy="zero") if df[col].dtype in [pl.Int64, pl.Float64]
                else pl.col(col).fill_null("") if df[col].dtype == pl.Utf8
                else pl.col(col)
                for col in df.columns
            ])

            if days is not None:
                now = dt.datetime.now()
                start_time = (now -dt.timedelta(days=days)).replace(hour=0,minute=0,second=0,microsecond=0)
                end_time = now.replace(hour=0,minute=0,second=0,microsecond=0)

            
            logging.info(f"过滤条件: {start_time} 到 {end_time}")
            # 确保日期列是datetime类型后再进行过滤
            filtered_df = df.filter(
                (pl.col(date_column).cast(pl.Datetime) >= start_time) & 
                (pl.col(date_column).cast(pl.Datetime) <= end_time)
            )
            logging.info(f"过滤后数据形状: {filtered_df.shape}")
        
            return filtered_df
           
        except Exception as e:
            logging.error(f"日期过滤失败: {str(e)}")
            logging.error(f"Debug - Column content: {df[date_column].head()}")
            return self.dataframe
        

    def add_date_only_column(self, 
                            date_column: str, 
                            new_column: str, 
                            format: str = "%Y/%m/%d") -> pl.DataFrame:
        """添加仅包含日期的新列"""
        try:
            # 确保日期列是datetime类型
            df = self.dataframe
            if df[date_column].dtype == pl.Utf8:
                df = df.with_columns([
                    pl.col(date_column).str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False)
                ])
            
            return df.with_columns([
                pl.col(date_column).dt.strftime(format).alias(new_column)
            ])
        except Exception as e:
            return ValueError(f"列{date_column}的日期格式不正确，请检查数据格式")
        
    
    def drop_columns_after(self,column_name:str)->pl.DataFrame:
        ### 删除指定列之后的所有列 ###
        if column_name in self.dataframe.columns:
            column_index = self.dataframe.columns.index(column_name)
            return self.dataframe.select(self.dataframe.columns[:column_index+1])
        return self.dataframe
    
    def clean_and_unique(self,unique_columns:Union[str,List[str]],drop_columns:List[str]=None)->pl.DataFrame:
        ### 清洗数据并去重 ###
        df = self.dataframe
        if drop_columns:
            df = df.drop(drop_columns)
        return df.unique(subset=unique_columns if isinstance(unique_columns,list) else [unique_columns])
        
    def combine_columns(self, 
                       columns: List[str], 
                       separator: str = "-", 
                       new_column: str = None) -> pl.DataFrame:
        """合并多个列，处理可能的空值和类型转换问题
        
        Args:
            columns: 要合并的列名列表
            separator: 分隔符，默认为"-"
            new_column: 新列的名称，如果为None则使用列名组合
        """
        try:
            if all(col in self.dataframe.columns for col in columns):
                # 首先处理每一列，确保类型转换和空值处理
                df = self.dataframe
                for col in columns:
                    df = df.with_columns([
                        pl.col(col).fill_null("").cast(pl.Utf8).alias(col)
                    ])
                
                # 使用处理后的列进行合并
                combined = pl.concat_str(
                    [pl.col(col) for col in columns],
                    separator=separator
                )
                
                return df.with_columns([
                    combined.alias(new_column or separator.join(columns))
                ])
            return self.dataframe
        except Exception as e:
            logging.error(f"合并列失败: {str(e)}")
            return self.dataframe


    def calculate_repeat_counts(self, 
                              group_column: str, 
                              count_column: str = "重复次数") -> pl.DataFrame:
        """计算重复次数"""
        counts = self.dataframe.group_by(group_column).agg(pl.len().alias(count_column))
        return self.dataframe.join(counts, on=group_column, how="left")
