import polars as pl
from tool.file import FileManager


def categorize_city_by_station_id(df: pl.DataFrame, station_id_column: str = "基站号") -> pl.DataFrame:
    # 使用 when().then().otherwise() 结构来根据基站ID范围划分地市
    df = df.with_columns(
        pl.when(
            pl.col(station_id_column).is_between(565248, 567295, closed="both") |
            pl.col(station_id_column).is_between(844928, 845183, closed="both") |
            pl.col(station_id_column).is_between(840832, 841087, closed="both") |
            pl.col(station_id_column).is_between(983040, 983551, closed="both") |
            pl.col(station_id_column).is_between(985344, 985695, closed="both")
        )
        .then(pl.lit("抚州"))
        .when(
            pl.col(station_id_column).is_between(552960, 556287, closed="both") |
            pl.col(station_id_column).is_between(843776, 844159, closed="both") |
            pl.col(station_id_column).is_between(839680, 840063, closed="both") |
            pl.col(station_id_column).is_between(556288, 556587, closed="both") |
            pl.col(station_id_column).is_between(985856, 986367, closed="both")
        )
        .then(pl.lit("南昌"))
        .when(
            pl.col(station_id_column).is_between(557056, 559871, closed="both") |
            pl.col(station_id_column).is_between(844160, 844543, closed="both") |
            pl.col(station_id_column).is_between(840064, 840447, closed="both")
        )
        .then(pl.lit("九江"))
        .when(
             pl.col(station_id_column).is_between(569344, 572159, closed="both") |
            pl.col(station_id_column).is_between(845184, 845567, closed="both") |
            pl.col(station_id_column).is_between(841088, 841471, closed="both")
        )
        .then(pl.lit("宜春"))
        .when(
            pl.col(station_id_column).is_between(567296, 569343, closed="both") |
            pl.col(station_id_column).is_between(845568, 845951, closed="both") |
            pl.col(station_id_column).is_between(841472, 841855, closed="both") |
            pl.col(station_id_column).is_between(984576, 985087, closed="both")
        )
        .then(pl.lit("吉安"))
        .when(
            pl.col(station_id_column).is_between(573440, 576511, closed="both") |
            pl.col(station_id_column).is_between(845952, 846463, closed="both") |
            pl.col(station_id_column).is_between(841856, 842367, closed="both") |
            pl.col(station_id_column).is_between(556588, 557055, closed="both") |
            pl.col(station_id_column).is_between(983552, 984575, closed="both")
        )
        .then(pl.lit("赣州"))
        .when(
            pl.col(station_id_column).is_between(559872, 561151, closed="both") |
            pl.col(station_id_column).is_between(846464, 846591, closed="both") |
            pl.col(station_id_column).is_between(842368, 842495, closed="both")
        )
        .then(pl.lit("景德镇"))
        .when(
            pl.col(station_id_column).is_between(563968, 565247, closed="both") |
            pl.col(station_id_column).is_between(846592, 846719, closed="both") |
            pl.col(station_id_column).is_between(842496, 842623, closed="both")
        )
        .then(pl.lit("萍乡"))
        .when(
            pl.col(station_id_column).is_between(576512, 577535, closed="both") |
            pl.col(station_id_column).is_between(846720, 846847, closed="both") |
            pl.col(station_id_column).is_between(842624, 842751, closed="both")
        )
        .then(pl.lit("新余"))
        .when(
            pl.col(station_id_column).is_between(572160, 573439, closed="both") |
            pl.col(station_id_column).is_between(846848, 846975, closed="both") |
            pl.col(station_id_column).is_between(842752, 842879, closed="both")
        )
        .then(pl.lit("鹰潭"))
        .when(
            pl.col(station_id_column).is_between(846976, 847871, closed="both") |
            pl.col(station_id_column).is_between(842880, 843775, closed="both")
        )
        .then(pl.lit("共享预留"))
        .when(
            pl.col(station_id_column).is_between(986368, 987135, closed="both")
        )
        .then(pl.lit("预留"))
        .otherwise(pl.lit(None))  # 如果不在范围内，则设置为空值 (None)
        .alias("地市")
    )
    return df


if __name__ == "__main__":
    file_Manager = FileManager("WorkDocument\\刘辉")
    df = file_Manager.read_csv(file_name="4G MR联通50周.csv")
    df = categorize_city_by_station_id(df)
    file_Manager.save_to_sheet("4G MR联通50周_地市", 地市数据=df)

    print(df)
