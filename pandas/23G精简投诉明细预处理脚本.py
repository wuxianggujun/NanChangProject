import datetime as dt
import logging
import os
import pandas as pd

import Utils

# 设置日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == '__main__':
    startTime = dt.datetime.now()
    logging.info("开始解析工单查询数据并生成23G精简投诉明细数据....")
    current_path = os.path.dirname(os.path.abspath(__file__))
    source_dir = os.path.join(current_path, "WorkDocument", "23G精简投诉明细预处理脚本", "source")

    if not os.path.exists(source_dir):
        logging.error("未找到指定目录，请检查路径是否正确！")
        exit(1)

    file_path = Utils.get_first_excel_file_in_dir(source_dir)

    if file_path:
        if not file_path:
            logging.error("文件路径为空，无法处理...")
            exit(1)
        excel_data = Utils.read_excel(file_path)

        if excel_data is not None:
            logging.info("如果本周一，记得修改days参数为3天否则默认为1，表示前一天的数据。")
            
            processed_data = Utils.process_dataframe(excel_data, days=1)
           
            data_str = startTime.strftime("%Y%m%d")
            output_filename = f"23G精简投诉明细_预处理_{data_str}{os.path.splitext(file_path)[1]}"
            output_path = os.path.join(current_path, "WorkDocument", "23G精简投诉明细预处理脚本", output_filename)
            Utils.save_to_excel(processed_data, output_path)
        else:
            logging.error("解析工单查询数据失败")

    end_time = dt.datetime.now()
    logging.info("解析工单查询数据耗时：%s" % (end_time - startTime))
