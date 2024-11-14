import polars as pl


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
