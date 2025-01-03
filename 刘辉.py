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
import yaml

with open("WorkDocument\\刘辉\\config\\config.yaml","r",encoding="utf-8") as f:
    config = yaml.safe_load(f)

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
        mode:str,
        station_id_column: str = "基站号",
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """根据基站号范围判断地市"""
    if station_id_column not in df.columns:
        raise ValueError(f"数据中缺少必要的列: {station_id_column}")
        # 选择对应运营商和模式的基站号范围配置
    if operator_suffix == "联通自建":
        operator_ranges = station_id_ranges.get("联通", {}).get(mode, [])
    elif operator_suffix == "电信自建":
        operator_ranges = station_id_ranges.get("电信", {}).get(mode, [])
    else:
        raise ValueError(f"未知的运营商: {operator_suffix}")
        
    # 使用表达式链构建条件
    when_expr = None
    for city_data in operator_ranges:  # 遍历数组表格
        city = city_data['name']
        condition = None
        for start, end in city_data['ranges']:
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

def process_cqi_data(df: pl.DataFrame, station_id_ranges: dict,mode:str, station_id_column: str = "基站号") -> pl.DataFrame:
    """
    根据基站号划分地市，并累计 CQI >=7 到 CQI 15 的数据

    Args:
        df: 包含基站号和 CQI 数据的 DataFrame
        station_id_ranges: 联通基站号范围字典
        station_id_column: 基站号所在的列名

    Returns:
        按地市汇总的 CQI 数据 DataFrame
    """
    # 过滤指定 mode 的基站号范围
    operator_ranges = station_id_ranges.get("联通", {}).get(mode, [])
    # 根据地市范围划分地市
    when_expr = None
    for city_data in operator_ranges:  # 遍历数组表格
        city = city_data['name']
        condition = None
        for start, end in city_data['ranges']:
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

    # 过滤掉地市为空的行
    df = df.filter(pl.col("地市").is_not_null())

    # 累加 CQI >=7 到 CQI 15 的数据
    cqi_columns = [f"CQI {i}数量(个)" for i in range(7, 16)]  # CQI 7 到 CQI 15
    df = df.with_columns(
        [pl.col(col).cast(pl.Int64).fill_null(0) for col in cqi_columns]
    )

    # 计算 CQI>=7 的总和
    df = df.with_columns(
        pl.sum_horizontal(*cqi_columns).alias("CQI>=7数量")
    )

    # 累加 CQI 0 到 CQI 15 的数据
    all_cqi_columns = [f"CQI {i}数量(个)" for i in range(0, 16)]
    df = df.with_columns(
        [pl.col(col).cast(pl.Int64).fill_null(0) for col in all_cqi_columns]
    )

    # 计算 CQI 总数
    df = df.with_columns(
        pl.sum_horizontal(*all_cqi_columns).alias("CQI总数")
    )
    
    # 按地市汇总 CQI>=7 数据 和 CQI总数
    cqi_stats = df.group_by("地市").agg(
        [
            pl.col("CQI>=7数量").sum().alias("CQI>=7数量"),
            pl.col("CQI总数").sum().alias("CQI总数"),
        ]
    )

    return cqi_stats
    
def fill_cqi_to_weekly_stats(weekly_stats: pl.DataFrame, cqi_stats: pl.DataFrame) -> pl.DataFrame:
    """
      将 CQI>=7 数据填充到 4G 周指标表中

      Args:
          weekly_stats: 4G 周指标表
          cqi_stats: 按地市汇总的 CQI 数据

      Returns:
          更新后的 4G 周指标表
      """
    # 将 weekly_stats 的 "城市名称" 列转换为 String 类型
    weekly_stats = weekly_stats.with_columns(
        pl.col("城市名称").cast(pl.String)
    )

    # 将 cqi_stats 的 "地市" 列转换为 String 类型
    cqi_stats = cqi_stats.with_columns(
        pl.col("地市").cast(pl.String)
    )

    # 将 CQI 数据合并到 4G 周指标表中
    weekly_stats = weekly_stats.join(
        cqi_stats, left_on="城市名称", right_on="地市", how="left"
    ).with_columns(
        [
            pl.col("CQI>=7数量_right").fill_null(0).alias("CQI>=7数量"),  # 将 CQI>=7数量_right 合并到 CQI>=7数量
            pl.col("CQI总数_right").fill_null(0).alias("CQI总数")  # 将 CQI总数_right 合并到 CQI总数
        ]
    ).drop(["CQI>=7数量_right", "CQI总数_right"])  # 删除多余的列

    # 计算 4G 优良率
    weekly_stats = weekly_stats.with_columns(
        (pl.col("CQI>=7数量") / pl.col("CQI总数")).alias("CQI优良率")
    )
    
    # 按地市排序
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
    
    # 计算全省的 CQI>=7 数量总和、CQI 总数总和
    province_cqi_ge7_sum = weekly_stats.filter(pl.col("城市名称") != "全省")["CQI>=7数量"].sum()
    province_cqi_total_sum = weekly_stats.filter(pl.col("城市名称") != "全省")["CQI总数"].sum()

    # 计算全省的 CQI 优良率
    province_cqi_ratio = province_cqi_ge7_sum / province_cqi_total_sum if province_cqi_total_sum else 0.0

    # 更新全省行的数据
    weekly_stats = weekly_stats.with_columns(
        pl.when(pl.col("城市名称") == "全省")
        .then(
            pl.Series(
                name="CQI>=7数量",
                values=[province_cqi_ge7_sum],
                dtype=pl.Float64,  # 使用统计的数据类型
            )
        )
        .otherwise(pl.col("CQI>=7数量"))
        .alias("CQI>=7数量")
    )

    weekly_stats = weekly_stats.with_columns(
        pl.when(pl.col("城市名称") == "全省")
        .then(
            pl.Series(
                name="CQI总数",
                values=[province_cqi_total_sum],
                dtype=pl.Float64,  # 使用统计的数据类型
            )
        )
        .otherwise(pl.col("CQI总数"))
        .alias("CQI总数")
    )

    weekly_stats = weekly_stats.with_columns(
        pl.when(pl.col("城市名称") == "全省")
        .then(pl.lit(province_cqi_ratio))
        .otherwise(pl.col("CQI优良率"))
        .alias("CQI优良率")
    )

    # Add a temporary column with the sort order using a join
    order_df = pl.DataFrame({"城市名称": city_order, "city_order": range(len(city_order))})
    weekly_stats = weekly_stats.join(order_df, on="城市名称", how="left")

    # Sort by the temporary column
    weekly_stats = weekly_stats.sort("city_order")

    # Remove the temporary column
    weekly_stats = weekly_stats.drop("city_order")

    return weekly_stats
    

if __name__ == "__main__":
    start_time = time.time()

    # 指定模式 (4G 或 5G)
    mode = "4G"  # 你可以根据需要修改为 "5G"
    
    file_manager = ExcelManager(config["paths"]["working_directory"])

    print("开始处理数据...")
    # 根据 mode 选择路径配置
    paths = config["paths"][mode]
    
    # 基站号范围配置从配置文件加载
    # unicom_ranges = config["network_ranges"]["联通"][mode]
    # telecom_ranges = config["network_ranges"]["电信"][mode]
    network_ranges = config["network_ranges"]
    try:
        print(f"正在处理{mode}联通数据...")
        df_unicom = read_mr_data(
            file_manager, paths["unicom_mr_data"]
        )
        df_unicom, rsrp_stats_unicom = categorize_city_by_station_id(
            df_unicom, network_ranges, "联通自建", mode
        )

        print(f"正在处理{mode}电信数据...")
        df_telecom = read_mr_data(
            file_manager, paths["telecom_mr_data"]
        )
        df_telecom, rsrp_stats_telecom = categorize_city_by_station_id(
            df_telecom, network_ranges, "电信自建", mode
        )

        print(f"正在生成{mode}周指标...")
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
        print(f"正在读取并解析{mode}新的CSV文件...")
        df_perf = file_manager.read_csv(file_name=paths["perf_query_data"], encoding="gbk")
        df_perf = parse_station_id(df_perf, "对象编号")

        cqi_stats = process_cqi_data(df_perf,network_ranges,mode)

        # 将 CQI 数据合并到 4G 周指标表中
        weekly_4g_stats = fill_cqi_to_weekly_stats(weekly_4g_stats, cqi_stats)
        
        save_results(
            file_manager,
            {
                "weekly_4g_stats": weekly_4g_stats,
                "rsrp_stats_unicom": rsrp_stats_unicom,
                "rsrp_stats_telecom": rsrp_stats_telecom,
                "df_unicom": df_unicom_save,
                "df_telecom": df_telecom_save,
                "perf_query": df_perf
            },
            paths["output_base_name"],
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
