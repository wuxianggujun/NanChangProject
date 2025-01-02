import logging

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


def categorize_city_by_station_id(
        df: pl.DataFrame,
        station_id_ranges: dict,
        operator_suffix: str,
        station_id_column: str = "基站号",
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """根据基站号范围判断地市"""
    if station_id_column not in df.columns:
        raise ValueError(f"数据中缺少必要的列: {station_id_column}")

    # 使用表达式链构建条件
    when_expr = None
    for city, ranges in station_id_ranges.items():
        condition = None
        for start, end in ranges:
            if condition is None:
                condition = pl.col(station_id_column).is_between(
                    start, end, closed="both"
                )
            else:
                condition = condition | pl.col(station_id_column).is_between(
                    start, end, closed="both"
                )

        if when_expr is None:
            when_expr = pl.when(condition).then(pl.lit(city))
        else:
            when_expr = when_expr.when(condition).then(pl.lit(city))

    # 添加地市列
    df = df.with_columns(when_expr.otherwise(pl.lit(None)).alias("地市"))

    rsrp_stats = aggregate_rsrp_by_city(df, operator_suffix)
    return df, rsrp_stats


def aggregate_rsrp_by_city(df: pl.DataFrame, operator_suffix: str) -> pl.DataFrame:
    """按地市统计 MRO-RSRP 数据"""
    stats = (
        df.group_by("地市")
        .agg(
            [
                pl.col("MRO-RSRP≥-112采样点数")
                .cast(pl.Int64)
                .sum()
                .alias(f"MRO-RSRP≥-112采样点数({operator_suffix})"),
                pl.col("MRO-RSRP总采样点数")
                .cast(pl.Int64)
                .sum()
                .alias(f"MRO-RSRP总采样点数({operator_suffix})"),
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
        "抚州",
        "赣州",
        "吉安",
        "景德镇",
        "九江",
        "南昌",
        "萍乡",
        "上饶",
        "新余",
        "宜春",
        "鹰潭",
        "全省",
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
    merged_stats = merged_stats.select(
        [
            "城市名称",
            "MRO-RSRP≥-112采样点数(联通自建)",
            "MRO-RSRP总采样点数(联通自建)",
            "RSRP≥-112采样点占比(联通自建)",
            "MRO-RSRP≥-112采样点数(电信自建)",
            "MRO-RSRP总采样点数(电信自建)",
            "RSRP≥-112采样点占比(电信共入)",
        ]
    )

    # 计算全省的汇总数据
    total_unicom = merged_stats["MRO-RSRP≥-112采样点数(联通自建)"].sum()
    total_unicom_samples = merged_stats["MRO-RSRP总采样点数(联通自建)"].sum()
    total_telecom = merged_stats["MRO-RSRP≥-112采样点数(电信自建)"].sum()
    total_telecom_samples = merged_stats["MRO-RSRP总采样点数(电信自建)"].sum()

    # 计算全省占比
    province_unicom_ratio = (
        round(total_unicom * 100.0 / total_unicom_samples, 2)
        if total_unicom_samples > 0
        else 0
    )
    province_telecom_ratio = (
        round(total_telecom * 100.0 / total_telecom_samples, 2)
        if total_telecom_samples > 0
        else 0
    )

    # 创建全省统计数据
    province_stats = pl.DataFrame(
        {
            "城市名称": ["全省"],
            "MRO-RSRP≥-112采样点数(联通自建)": [total_unicom],
            "MRO-RSRP总采样点数(联通自建)": [total_unicom_samples],
            "RSRP≥-112采样点占比(联通自建)": [province_unicom_ratio],
            "MRO-RSRP≥-112采样点数(电信自建)": [total_telecom],
            "MRO-RSRP总采样点数(电信自建)": [total_telecom_samples],
            "RSRP≥-112采样点占比(电信共入)": [province_telecom_ratio],
        }
    )

    # 合并地市数据和全省数据
    final_stats = pl.concat([merged_stats, province_stats])

    # 使用旧版 Polars 兼容的方式进行排序
    final_stats = final_stats.with_columns(
        pl.col("城市名称").cast(pl.Categorical)
    ).sort(by=pl.Series(values=city_order, name="城市名称"))

    # 添加空列以匹配目标Excel格式
    final_stats = final_stats.with_columns(
        [
            pl.lit(None).alias("MRO-RSRP≥-112采样点数"),
            pl.lit(None).alias("MRO-RSRP总采样点数"),
            pl.lit(None).alias("MRO-RSRP≥-112采样点占比"),
            pl.lit(None).alias("CQI>=7数量"),
            pl.lit(None).alias("CQI总数"),
            pl.lit(None).alias("CQI优良率"),
        ]
    )

    # 调整列的顺序以匹配目标Excel格式
    final_stats = final_stats.select(
        [
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
        ]
    )
    return final_stats


def calculate_and_fill_missing_columns(df: pl.DataFrame) -> pl.DataFrame:
    """计算并填充缺失的列"""
    return df.with_columns(
        [
            (
                    pl.col("MRO-RSRP≥-112采样点数(联通自建)") + pl.col("MRO-RSRP≥-112采样点数(电信自建)")
            ).alias("MRO-RSRP≥-112采样点数"),
            (pl.col("MRO-RSRP总采样点数(联通自建)") + pl.col("MRO-RSRP总采样点数(电信自建)")).alias(
                "MRO-RSRP总采样点数"
            ),
        ]
    ).with_columns(
        (pl.col("MRO-RSRP≥-112采样点数") * 100.0 / pl.col("MRO-RSRP总采样点数")).alias(
            "MRO-RSRP≥-112采样点占比"
        )
    ).with_columns(
        [
            pl.lit(None).alias("CQI>=7数量"),  # 占位符，根据实际数据填充
            pl.lit(None).alias("CQI总数"),  # 占位符，根据实际数据填充
            pl.lit(None).alias("CQI优良率"),  # 占位符，根据实际数据填充
        ]
    )


def save_results(file_manager: ExcelManager, results: dict, base_name: str):
    """保存结果到不同格式"""
    # 保存主要统计结果为Excel
    file_manager.save_multiple_sheets(
        filename=f"{base_name}_统计结果",
        # formatter=ExcelFormatter,
        _4G周指标=results["weekly_4g_stats"],
        联通指标统计=results["rsrp_stats_unicom"],
        电信指标统计=results["rsrp_stats_telecom"],
        perf_query=results["perf_query"],
    )


def parse_station_id(df: pl.DataFrame, column_name: str = "对象编号") -> pl.DataFrame:
    """
    解析对象编号列，提取基站号

    Args:
        df: 包含对象编号的DataFrame
        column_name: 对象编号所在的列名

    Returns:
        包含基站号的新DataFrame
    """

    # 定义一个函数来解析基站号
    def extract_station_id(id_str: str) -> int:
        if "." in id_str:
            parts = id_str.split(".")
            if len(parts) >= 2:
                try:
                    return int(parts[1])
                except ValueError:
                    return None
        return None

    # 去除字符串两端的空格和不可见字符
    df = df.with_columns(
        pl.col(column_name).str.replace(r"^\s+|\s+$", "")
    )

    print(df.filter(pl.col("对象编号") == "112.1.0"))
    
    df_filtered = df.filter(
        ~pl.col(column_name).str.contains(r"^112\.1\.0$")  # 过滤掉 "112.1.0"
        & ~pl.col(column_name).str.contains(r"^112\.1\.DC=")  # 过滤掉以 "112.1.DC=" 开头的行
        & ~pl.col(column_name).str.contains(r"^201")  # 过滤掉以 "201" 开头的行
        & ~pl.col(column_name).str.contains(
            r"^112\.SubNetwork=ZTE_UME_SYSTEM")  # 过滤掉以 "112.SubNetwork=ZTE_UME_SYSTEM" 开头的行
        & ~pl.col(column_name).str.contains(r"www\.zte\.com\.cn$")  # 过滤掉以 "www.zte.com.cn" 结尾的行
    )

    # 3. 尝试解析基站号
    df_with_station_id = df_filtered.with_columns(
        pl.col(column_name).map_elements(extract_station_id, return_dtype=pl.Int64).alias("基站号")
    )
    
    # 4. 删除基站号为空的行
    df_result = df_with_station_id.filter(
        pl.col("基站号").is_not_null()  # 保留基站号不为空的行
    )

    return df_result


if __name__ == "__main__":
    start_time = time.time()

    file_manager = ExcelManager("WorkDocument\\刘辉")

    print("开始处理数据...")

    # 基站号范围配置
    unicom_ranges = {
        "抚州": [
            (565248, 567295),
            (844928, 845183),
            (840832, 841087),
            (983040, 983551),
            (985344, 985695),
        ],
        "南昌": [
            (552960, 556287),
            (843776, 844159),
            (839680, 840063),
            (556288, 556587),
            (985856, 986367),
        ],
        "九江": [(557056, 559871), (844160, 844543), (840064, 840447)],
        "宜春": [(569344, 572159), (845184, 845567), (841088, 841471)],
        "吉安": [(567296, 569343), (845568, 845951), (841472, 841855), (984576, 985087)],
        "赣州": [
            (573440, 576511),
            (845952, 846463),
            (841856, 842367),
            (556588, 557055),
            (983552, 984575),
        ],
        "景德镇": [(559872, 561151), (846464, 846591), (842368, 842495)],
        "萍乡": [(563968, 565247), (846592, 846719), (842496, 842623)],
        "新余": [(576512, 577535), (846720, 846847), (842624, 842751)],
        "鹰潭": [(572160, 573439), (846848, 846975), (842752, 842879)],
        "上饶": [
            (561152, 563967),
            (844544, 844927),
            (840448, 840831),
            (985088, 985343),
        ],
        "共享预留": [(846976, 847871), (842880, 843775)],
        "预留": [(986368, 987135)],
    }

    telecom_ranges = {
        "南昌": [
            (450560, 454655),
            (473088, 473599),
            (476160, 476927),
            (477440, 477950),
            (823296, 825855),
            (839680, 840063),
            (843776, 844159),
            (478464, 478591),
        ],
        "九江": [
            (454656, 456703),
            (825856, 828415),
            (840064, 840447),
            (844160, 844543),
            (478592, 478671),
        ],
        "上饶": [
            (456704, 458751),
            (473600, 473983),
            (828416, 830975),
            (840448, 840831),
            (844544, 844927),
            (478672, 478751),
        ],
        "抚州": [
            (458752, 460799),
            (473984, 474367),
            (830976, 833535),
            (840832, 841087),
            (844928, 845183),
            (478752, 478831),
        ],
        "宜春": [
            (460800, 462847),
            (474368, 474751),
            (833536, 836095),
            (841088, 841471),
            (845184, 845567),
            (478832, 478911),
        ],
        "吉安": [
            (462848, 464895),
            (474752, 475135),
            (836096, 838655),
            (841472, 841855),
            (845568, 845951),
            (478912, 478991),
        ],
        "赣州": [
            (464896, 466943),
            (475136, 475519),
            (838656, 841215),
            (841856, 842367),
            (845952, 846463),
            (478992, 479071),
        ],
        "景德镇": [
            (466944, 468991),
            (475520, 475903),
            (841216, 841471),
            (842368, 842495),
            (846464, 846591),
            (479072, 479151),
        ],
        "萍乡": [
            (468992, 471039),
            (475904, 476287),
            (841472, 841855),
            (842496, 842623),
            (846592, 846719),
            (479152, 479231),
        ],
        "新余": [
            (471040, 473087),
            (476288, 476927),
            (841856, 842367),
            (842624, 842751),
            (846720, 846847),
            (479232, 479311),
        ],
        "鹰潭": [
            (473088, 475135),
            (476928, 477439),
            (842368, 842879),
            (842752, 842879),
            (846848, 846975),
            (479312, 479391),
        ],
        "共享预留": [(475136, 477439), (842880, 843775)],
        "预留": [(477440, 478463)],
    }

    try:
        print("正在处理联通数据...")
        df_unicom = read_mr_data(file_manager, "4G MR联通50周.csv")
        df_unicom, rsrp_stats_unicom = categorize_city_by_station_id(
            df_unicom, unicom_ranges, "联通自建"
        )

        print("正在处理电信数据...")
        df_telecom = read_mr_data(file_manager, "4G MR电信50周.csv")
        df_telecom, rsrp_stats_telecom = categorize_city_by_station_id(
            df_telecom, telecom_ranges, "电信自建"
        )

        print("正在生成4G周指标...")
        weekly_4g_stats = create_weekly_4g_stats_for_excel(
            rsrp_stats_unicom, rsrp_stats_telecom
        )

        weekly_4g_stats = calculate_and_fill_missing_columns(weekly_4g_stats)

        # 选择需要的列
        df_unicom_save = df_unicom.select(
            ["地市", "基站号", "MRO-RSRP≥-112采样点数", "MRO-RSRP总采样点数"]
        )
        df_telecom_save = df_telecom.select(
            ["地市", "基站号", "MRO-RSRP≥-112采样点数", "MRO-RSRP总采样点数"]
        )

        # 读取并解析新的CSV文件
        print("正在读取并解析新的CSV文件...")
        df_perf = file_manager.read_csv(file_name="perf_query_result20241210103124.csv", encoding="gbk")
        df_perf = parse_station_id(df_perf, "对象编号")

        print("正在保存处理结果...")
        save_results(
            file_manager,
            {
                "weekly_4g_stats": weekly_4g_stats,
                "rsrp_stats_unicom": rsrp_stats_unicom,
                "rsrp_stats_telecom": rsrp_stats_telecom,
                "df_unicom": df_unicom_save,
                "df_telecom": df_telecom_save,
                "perf_query": df_perf,
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
