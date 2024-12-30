import polars as pl
from tool.file import ExcelManager  # 假设你的 ExcelManager 类在 excel_manager.py 文件中
import time
from datetime import timedelta
import os
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Side, Font, PatternFill
from openpyxl.utils import get_column_letter
from openpyxl.utils.cell import range_boundaries
from openpyxl.cell import MergedCell


def read_mr_data(file_manager: ExcelManager, filename: str) -> pl.DataFrame:
    """读取 MR 数据，处理数据类型问题"""
    return file_manager.read_csv(
        file_name=filename,
        infer_schema_length=10000,  # 增加推断模式的长度
        try_parse_dates=True,  # 尝试解析日期
        schema_overrides={
            "移动-平均RSRP": pl.Float64,  # 确保 RSRP 值被解析为浮点数
            "基站号": pl.Int64,  # 确保基站号被解析为整数
        },
    )


def categorize_city_by_station_id_unicom(
        df: pl.DataFrame, station_id_column: str = "基站号"
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """联通基站号范围判断"""
    if station_id_column not in df.columns:
        raise ValueError(f"数据中缺少必要的列: {station_id_column}")

    df = df.with_columns(
        pl.when(
            pl.col(station_id_column).is_between(565248, 567295, closed="both")
            | pl.col(station_id_column).is_between(844928, 845183, closed="both")
            | pl.col(station_id_column).is_between(840832, 841087, closed="both")
            | pl.col(station_id_column).is_between(983040, 983551, closed="both")
            | pl.col(station_id_column).is_between(985344, 985695, closed="both")
        )
        .then(pl.lit("抚州"))
        .when(
            pl.col(station_id_column).is_between(552960, 556287, closed="both")
            | pl.col(station_id_column).is_between(843776, 844159, closed="both")
            | pl.col(station_id_column).is_between(839680, 840063, closed="both")
            | pl.col(station_id_column).is_between(556288, 556587, closed="both")
            | pl.col(station_id_column).is_between(985856, 986367, closed="both")
        )
        .then(pl.lit("南昌"))
        .when(
            pl.col(station_id_column).is_between(557056, 559871, closed="both")
            | pl.col(station_id_column).is_between(844160, 844543, closed="both")
            | pl.col(station_id_column).is_between(840064, 840447, closed="both")
        )
        .then(pl.lit("九江"))
        .when(
            pl.col(station_id_column).is_between(569344, 572159, closed="both")
            | pl.col(station_id_column).is_between(845184, 845567, closed="both")
            | pl.col(station_id_column).is_between(841088, 841471, closed="both")
        )
        .then(pl.lit("宜春"))
        .when(
            pl.col(station_id_column).is_between(567296, 569343, closed="both")
            | pl.col(station_id_column).is_between(845568, 845951, closed="both")
            | pl.col(station_id_column).is_between(841472, 841855, closed="both")
            | pl.col(station_id_column).is_between(984576, 985087, closed="both")
        )
        .then(pl.lit("吉安"))
        .when(
            pl.col(station_id_column).is_between(573440, 576511, closed="both")
            | pl.col(station_id_column).is_between(845952, 846463, closed="both")
            | pl.col(station_id_column).is_between(841856, 842367, closed="both")
            | pl.col(station_id_column).is_between(556588, 557055, closed="both")
            | pl.col(station_id_column).is_between(983552, 984575, closed="both")
        )
        .then(pl.lit("赣州"))
        .when(
            pl.col(station_id_column).is_between(559872, 561151, closed="both")
            | pl.col(station_id_column).is_between(846464, 846591, closed="both")
            | pl.col(station_id_column).is_between(842368, 842495, closed="both")
        )
        .then(pl.lit("景德镇"))
        .when(
            pl.col(station_id_column).is_between(563968, 565247, closed="both")
            | pl.col(station_id_column).is_between(846592, 846719, closed="both")
            | pl.col(station_id_column).is_between(842496, 842623, closed="both")
        )
        .then(pl.lit("萍乡"))
        .when(
            pl.col(station_id_column).is_between(576512, 577535, closed="both")
            | pl.col(station_id_column).is_between(846720, 846847, closed="both")
            | pl.col(station_id_column).is_between(842624, 842751, closed="both")
        )
        .then(pl.lit("新余"))
        .when(
            pl.col(station_id_column).is_between(572160, 573439, closed="both")
            | pl.col(station_id_column).is_between(846848, 846975, closed="both")
            | pl.col(station_id_column).is_between(842752, 842879, closed="both")
        )
        .then(pl.lit("鹰潭"))
        .when(
            pl.col(station_id_column).is_between(561152, 563967, closed="both")
            | pl.col(station_id_column).is_between(844544, 844927, closed="both")
            | pl.col(station_id_column).is_between(840448, 840831, closed="both")
            | pl.col(station_id_column).is_between(985088, 985343, closed="both")
        )
        .then(pl.lit("上饶"))
        .when(
            pl.col(station_id_column).is_between(846976, 847871, closed="both")
            | pl.col(station_id_column).is_between(842880, 843775, closed="both")
        )
        .then(pl.lit("共享预留"))
        .when(
            pl.col(station_id_column).is_between(986368, 987135, closed="both")
        )
        .then(pl.lit("预留"))
        .otherwise(pl.lit(None))
        .alias("地市")
    )

    rsrp_stats = aggregate_rsrp_by_city(df, "联通自建")
    return df, rsrp_stats


def categorize_city_by_station_id_telecom(
        df: pl.DataFrame, station_id_column: str = "基站号"
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """电信基站号范围判断"""
    if station_id_column not in df.columns:
        raise ValueError(f"数据中缺少必要的列: {station_id_column}")

    df = df.with_columns(
        pl.when(
            pl.col(station_id_column).is_between(450560, 454655, closed="both")
            | pl.col(station_id_column).is_between(473088, 473599, closed="both")
            | pl.col(station_id_column).is_between(476160, 476927, closed="both")
            | pl.col(station_id_column).is_between(477440, 477950, closed="both")
            | pl.col(station_id_column).is_between(823296, 825855, closed="both")
            | pl.col(station_id_column).is_between(839680, 840063, closed="both")
            | pl.col(station_id_column).is_between(843776, 844159, closed="both")
            | pl.col(station_id_column).is_between(478464, 478591, closed="both")
        )
        .then(pl.lit("南昌"))
        .when(
            pl.col(station_id_column).is_between(454656, 456703, closed="both")
            | pl.col(station_id_column).is_between(825856, 828415, closed="both")
            | pl.col(station_id_column).is_between(840064, 840447, closed="both")
            | pl.col(station_id_column).is_between(844160, 844543, closed="both")
            | pl.col(station_id_column).is_between(478592, 478671, closed="both")
        )
        .then(pl.lit("九江"))
        .when(
            pl.col(station_id_column).is_between(456704, 458751, closed="both")
            | pl.col(station_id_column).is_between(473600, 473983, closed="both")
            | pl.col(station_id_column).is_between(828416, 830975, closed="both")
            | pl.col(station_id_column).is_between(840448, 840831, closed="both")
            | pl.col(station_id_column).is_between(844544, 844927, closed="both")
            | pl.col(station_id_column).is_between(478672, 478751, closed="both")
        )
        .then(pl.lit("上饶"))
        .when(
            pl.col(station_id_column).is_between(458752, 460799, closed="both")
            | pl.col(station_id_column).is_between(473984, 474367, closed="both")
            | pl.col(station_id_column).is_between(830976, 833535, closed="both")
            | pl.col(station_id_column).is_between(840832, 841087, closed="both")
            | pl.col(station_id_column).is_between(844928, 845183, closed="both")
            | pl.col(station_id_column).is_between(478752, 478831, closed="both")
        )
        .then(pl.lit("抚州"))
        .when(
            pl.col(station_id_column).is_between(460800, 462847, closed="both")
            | pl.col(station_id_column).is_between(474368, 474751, closed="both")
            | pl.col(station_id_column).is_between(833536, 836095, closed="both")
            | pl.col(station_id_column).is_between(841088, 841471, closed="both")
            | pl.col(station_id_column).is_between(845184, 845567, closed="both")
            | pl.col(station_id_column).is_between(478832, 478911, closed="both")
        )
        .then(pl.lit("宜春"))
        .when(
            pl.col(station_id_column).is_between(462848, 464895, closed="both")
            | pl.col(station_id_column).is_between(474752, 475135, closed="both")
            | pl.col(station_id_column).is_between(836096, 838655, closed="both")
            | pl.col(station_id_column).is_between(841472, 841855, closed="both")
            | pl.col(station_id_column).is_between(845568, 845951, closed="both")
            | pl.col(station_id_column).is_between(478912, 478991, closed="both")
        )
        .then(pl.lit("吉安"))
        .when(
            pl.col(station_id_column).is_between(464896, 466943, closed="both")
            | pl.col(station_id_column).is_between(475136, 475519, closed="both")
            | pl.col(station_id_column).is_between(838656, 841215, closed="both")
            | pl.col(station_id_column).is_between(841856, 842367, closed="both")
            | pl.col(station_id_column).is_between(845952, 846463, closed="both")
            | pl.col(station_id_column).is_between(478992, 479071, closed="both")
        )
        .then(pl.lit("赣州"))
        .when(
            pl.col(station_id_column).is_between(466944, 468991, closed="both")
            | pl.col(station_id_column).is_between(475520, 475903, closed="both")
            | pl.col(station_id_column).is_between(841216, 841471, closed="both")
            | pl.col(station_id_column).is_between(842368, 842495, closed="both")
            | pl.col(station_id_column).is_between(846464, 846591, closed="both")
            | pl.col(station_id_column).is_between(479072, 479151, closed="both")
        )
        .then(pl.lit("景德镇"))
        .when(
            pl.col(station_id_column).is_between(468992, 471039, closed="both")
            | pl.col(station_id_column).is_between(475904, 476287, closed="both")
            | pl.col(station_id_column).is_between(841472, 841855, closed="both")
            | pl.col(station_id_column).is_between(842496, 842623, closed="both")
            | pl.col(station_id_column).is_between(846592, 846719, closed="both")
            | pl.col(station_id_column).is_between(479152, 479231, closed="both")
        )
        .then(pl.lit("萍乡"))
        .when(
            pl.col(station_id_column).is_between(471040, 473087, closed="both")
            | pl.col(station_id_column).is_between(476288, 476927, closed="both")
            | pl.col(station_id_column).is_between(841856, 842367, closed="both")
            | pl.col(station_id_column).is_between(842624, 842751, closed="both")
            | pl.col(station_id_column).is_between(846720, 846847, closed="both")
            | pl.col(station_id_column).is_between(479232, 479311, closed="both")
        )
        .then(pl.lit("新余"))
        .when(
            pl.col(station_id_column).is_between(473088, 475135, closed="both")
            | pl.col(station_id_column).is_between(476928, 477439, closed="both")
            | pl.col(station_id_column).is_between(842368, 842879, closed="both")
            | pl.col(station_id_column).is_between(842752, 842879, closed="both")
            | pl.col(station_id_column).is_between(846848, 846975, closed="both")
            | pl.col(station_id_column).is_between(479312, 479391, closed="both")
        )
        .then(pl.lit("鹰潭"))
        .when(
            pl.col(station_id_column).is_between(475136, 477439, closed="both")
            | pl.col(station_id_column).is_between(842880, 843775, closed="both")
        )
        .then(pl.lit("共享预留"))
        .when(
            pl.col(station_id_column).is_between(477440, 478463, closed="both")
        )
        .then(pl.lit("预留"))
        .otherwise(pl.lit(None))
        .alias("地市")
    )

    rsrp_stats = aggregate_rsrp_by_city(df, "电信自建")
    return df, rsrp_stats


def aggregate_rsrp_by_city(df: pl.DataFrame, operator_suffix: str) -> pl.DataFrame:
    """按地市统计 MRO-RSRP 数据"""
    stats = (
        df.group_by("地市")
        .agg(
            [
                pl.col("MRO-RSRP≥-112采样点数").cast(pl.Int64).sum().alias(f"MRO-RSRP≥-112采样点数({operator_suffix})"),
                pl.col("MRO-RSRP总采样点数").cast(pl.Int64).sum().alias(f"MRO-RSRP总采样点数({operator_suffix})"),
            ]
        )
        .filter(pl.col("地市").is_not_null())
    )

    return stats.select(
        [
            "地市",
            f"MRO-RSRP≥-112采样点数({operator_suffix})",
            f"MRO-RSRP总采样点数({operator_suffix})",
        ]
    )

def create_weekly_4g_stats_for_excel(
        rsrp_stats_unicom: pl.DataFrame, rsrp_stats_telecom: pl.DataFrame
) -> pl.DataFrame:
    """
    创建4G周指标表，用于生成Excel报表
    """
    city_order = [
        "抚州", "赣州", "吉安", "景德镇", "九江",
        "南昌", "萍乡", "上饶", "新余", "宜春",
        "鹰潭", "全省"
    ]

    # 预先计算各运营商的指标，并确保数据类型一致
    rsrp_stats_unicom = rsrp_stats_unicom.rename({"地市": "城市名称"}).with_columns(
        (
                pl.col("MRO-RSRP≥-112采样点数(联通自建)").cast(pl.Float64)
                * 100.0
                / pl.col("MRO-RSRP总采样点数(联通自建)").cast(pl.Float64)
        ).alias("RSRP≥-112采样点占比(联通自建)")
    )

    rsrp_stats_telecom = rsrp_stats_telecom.rename({"地市": "城市名称"}).with_columns(
        (
                pl.col("MRO-RSRP≥-112采样点数(电信自建)").cast(pl.Float64)
                * 100.0
                / pl.col("MRO-RSRP总采样点数(电信自建)").cast(pl.Float64)
        ).alias("RSRP≥-112采样点占比(电信共入)")
    )

    # 合并联通和电信的统计数据
    merged_stats = (
        rsrp_stats_unicom.join(rsrp_stats_telecom, on="城市名称", how="full")
        .fill_null(0)
        .sort("城市名称")
    )

    # 确保merged_stats包含所有必要的列
    merged_stats = merged_stats.select([
        "城市名称",
        "MRO-RSRP≥-112采样点数(联通自建)",
        "MRO-RSRP总采样点数(联通自建)",
        "RSRP≥-112采样点占比(联通自建)",
        "MRO-RSRP≥-112采样点数(电信自建)",
        "MRO-RSRP总采样点数(电信自建)",
        "RSRP≥-112采样点占比(电信共入)",
    ])

    # 计算全省的汇总数据
    total_unicom = merged_stats["MRO-RSRP≥-112采样点数(联通自建)"].sum()
    total_unicom_samples = merged_stats["MRO-RSRP总采样点数(联通自建)"].sum()
    total_telecom = merged_stats["MRO-RSRP≥-112采样点数(电信自建)"].sum()
    total_telecom_samples = merged_stats["MRO-RSRP总采样点数(电信自建)"].sum()

    # 创建全省统计数据
    province_stats = pl.DataFrame(
        {
            "城市名称": ["全省"],
            "MRO-RSRP≥-112采样点数(联通自建)": [total_unicom],
            "MRO-RSRP总采样点数(联通自建)": [total_unicom_samples],
            "RSRP≥-112采样点占比(联通自建)": [
                round(total_unicom * 100.0 / total_unicom_samples, 2)
                if total_unicom_samples > 0
                else 0
            ],
            "MRO-RSRP≥-112采样点数(电信自建)": [total_telecom],
            "MRO-RSRP总采样点数(电信自建)": [total_telecom_samples],
            "RSRP≥-112采样点占比(电信共入)": [
                round(total_telecom * 100.0 / total_telecom_samples, 2)
                if total_telecom_samples > 0
                else 0
            ],
        }
    )

    # 合并地市数据和全省数据
    final_stats = pl.concat([merged_stats, province_stats])

    # 使用 when-then 结构创建排序键
    sort_expr = pl.when(pl.col("城市名称") == "抚州").then(0)
    for i, city in enumerate(city_order[1:], 1):
        sort_expr = sort_expr.when(pl.col("城市名称") == city).then(i)
    sort_expr = sort_expr.otherwise(len(city_order))

    # 应用排序并选择最终需要的列
    final_stats = (
        final_stats.with_columns(sort_expr.alias("__sort_key"))
        .sort("__sort_key")
        .drop("__sort_key")
    )

    # 添加空列以匹配目标Excel格式
    final_stats = final_stats.with_columns([
        pl.lit(None).alias("MRO-RSRP≥-112采样点数"),
        pl.lit(None).alias("MRO-RSRP总采样点数"),
        pl.lit(None).alias("MRO-RSRP≥-112采样点占比"),
        pl.lit(None).alias("CQI>=7数量"),
        pl.lit(None).alias("CQI总数"),
        pl.lit(None).alias("CQI优良率"),
    ])

    # 调整列的顺序以匹配目标Excel格式
    final_stats = final_stats.select([
        "城市名称",
        "MRO-RSRP≥-112采样点数(联通自建)",
        "MRO-RSRP总采样点数(联通自建)",
        "RSRP≥-112采样点占比(联通自建)",
        "MRO-RSRP≥-112采样点数(电信自建)",
        "MRO-RSRP总采样点数(电信自建)",
        "RSRP≥-112采样点占比(电信共入)",
        "MRO-RSRP≥-112采样点数",
        "MRO-RSRP总采样点数",
        "MRO-RSRP≥-112采样点占比",
        "CQI>=7数量",
        "CQI总数",
        "CQI优良率",
    ])

    return final_stats


def save_results(file_manager: ExcelManager, results: dict, base_name: str):
    """保存结果到不同格式"""
    # 保存主要统计结果为Excel
    file_manager.save_multiple_sheets(
        filename=f"{base_name}_统计结果",
        # formatter=ExcelFormatter,
        _4G周指标=results["weekly_4g_stats"],
        联通指标统计=results["rsrp_stats_unicom"],
        电信指标统计=results["rsrp_stats_telecom"],
    )

    # # 保存大量原始数据为parquet格式
    # results["df_unicom"].write_parquet(
    #     os.path.join(file_manager.base_dir, f"{base_name}_联通数据.parquet")
    # )
    # results["df_telecom"].write_parquet(
    #     os.path.join(file_manager.base_dir, f"{base_name}_电信数据.parquet")
    # )


if __name__ == "__main__":
    start_time = time.time()

    file_manager = ExcelManager("WorkDocument\\刘辉")

    print("开始处理数据...")

    try:
        print("正在处理联通数据...")
        df_unicom = read_mr_data(file_manager, "4G MR联通50周.csv")
        df_unicom, rsrp_stats_unicom = categorize_city_by_station_id_unicom(df_unicom)

        print("正在处理电信数据...")
        df_telecom = read_mr_data(file_manager, "4G MR电信50周.csv")
        df_telecom, rsrp_stats_telecom = categorize_city_by_station_id_telecom(df_telecom)

        print("正在生成4G周指标...")
        weekly_4g_stats = create_weekly_4g_stats_for_excel(rsrp_stats_unicom, rsrp_stats_telecom)

        # 选择需要的列
        df_unicom_save = df_unicom.select(
            ["地市", "基站号", "MRO-RSRP≥-112采样点数", "MRO-RSRP总采样点数"]
        )
        df_telecom_save = df_telecom.select(
            ["地市", "基站号", "MRO-RSRP≥-112采样点数", "MRO-RSRP总采样点数"]
        )

        print("正在保存处理结果...")
        save_results(
            file_manager,
            {
                "weekly_4g_stats": weekly_4g_stats,
                "rsrp_stats_unicom": rsrp_stats_unicom,
                "rsrp_stats_telecom": rsrp_stats_telecom,
                "df_unicom": df_unicom_save,
                "df_telecom": df_telecom_save,
            },
            "4G MR数据分析",
        )

        end_time = time.time()
        execution_time = end_time - start_time
        formatted_time = str(timedelta(seconds=int(execution_time)))

        print(f"\n数据处理完成!")
        print(f"总运行时间: {formatted_time}")

    except Exception as e:
        print(f"\n处理过程中出现错误:")
        print(f"错误类型: {type(e).__name__}")
        print(f"错误信息: {str(e)}")

        end_time = time.time()
        execution_time = end_time - start_time
        formatted_time = str(timedelta(seconds=int(execution_time)))
        print(f"\n运行时间: {formatted_time}")
