import datetime as dt
import logging
import polars as pl
from tool.data.DataUtils import DataUtils
from tool.file import FileManager
from tool.decorators.ErrorHandler import error_handler


@error_handler
def process_complaints(df:pl.DataFrame)->pl.DataFrame:
    today_end = dt.datetime.now().replace(hour=17,minute=0,second=0,microsecond=0)
    yesterday_start = (today_end - dt.timedelta(days=1)).replace(hour=16,minute=0,second=0,microsecond=0)

    print(f"处理{yesterday_start}到{today_end}工单时间到")
    # 清理数据：删除全部列为空的行
    cleaned_df = df.filter(~pl.all_horizontal(pl.all().is_null()))

    datafframe = DataUtils(cleaned_df).filter_data_range(
        date_column="系统接单时间",
        start_time=yesterday_start,
        end_time=today_end
    )
        
    def classify_channel(channel:str)->str:
        if not channel or channel == "10015热线1" or channel == "10015升级投诉转电":
            return "10015"
        return channel
        
          
    # 确保所有列都是具体的值而不是表达式
    processed_df = datafframe.with_columns([
        pl.col("受理渠道").map_elements(classify_channel, return_dtype=pl.Utf8).alias("处理后渠道")
    ])

    # 执行分组统计
    stats = (
        processed_df
            .lazy()
            .group_by("区域", "处理后渠道")
            .agg([
                pl.len().alias("投诉数量")
            ])
            .collect()
        )

    return datafframe,stats
    
@error_handler
def generate_report_text(stats_df:pl.DataFrame)->str:
    total_complaints = stats_df["投诉数量"].sum()

    channel_totals = stats_df.group_by("处理后渠道").agg(
        pl.sum("投诉数量").alias("总数")
    ).to_dicts()

    complaints_10010 = next((item["总数"] for item in channel_totals if item["处理后渠道"] == "10010客服热线"),0)
    complaints_10015 = next((item["总数"] for item in channel_totals if item["处理后渠道"] == "10015"),0)

    region_stats = stats_df.group_by("区域").agg([
        pl.col("投诉数量").sum().alias("总数"),
        pl.col("投诉数量").filter(pl.col("处理后渠道") == "10010客服热线").sum().alias("10010数量"),
        pl.col("投诉数量").filter(pl.col("处理后渠道") == "10015").sum().alias("10015数量")
    ]).sort("总数",descending=True)

    date_str = dt.datetime.now().strftime("%Y%m%d")
    report_text = f"{date_str}地市工单总量{total_complaints}单（10010投诉{complaints_10010}单、10015投诉{complaints_10015}单），无集中投诉"

    # # TODO 这里可以过滤并且添加集中投诉的统计逻辑
    # concentrated_complaints = ""
    # if concentrated_complaints:
    #     region_text += f"，{concentrated_complaints}"

    # 获取投诉量大于10的地市,并且只需要投诉了前三的
    top_regions = region_stats.filter(
        pl.col("总数") > 10
    ).head(3).to_dicts()

    if top_regions:
        region_texts = []
        for region in top_regions:
            region_text = f"{region['区域']}{region['总数']}单（10010投诉{region['10010数量']}单、10015投诉{region['10015数量']}单）"
            region_texts.append(region_text)
        
        if region_texts:
             report_text += "，工单较多的地市：" + "，".join(region_texts)

    return report_text


if __name__ == "__main__":
    start_time = dt.datetime.now()
    file_manager = FileManager("WorkDocument\\日常日报")

    source_file = file_manager.get_latest_file("source")
    df = file_manager.read_excel(file_path= source_file)
    result_df,stats_df = process_complaints(df)
    report_text = generate_report_text(stats_df)



    text_df = pl.DataFrame({"日报信息":[report_text]})
    file_manager.save_to_sheet("日常日报",原始数据=result_df,统计结果=stats_df,日报信息=text_df)

    print(f"输出文件路径:{file_manager.output_path}")
    print("\n生成的日报信息文本:")
    print(report_text)

    end_time = dt.datetime.now()
    runtime = end_time - start_time
    logging.info(f"处理完成，总耗时: {runtime}秒")



