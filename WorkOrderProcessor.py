import time
import datetime as dt
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from tool.file import FileManager 
import polars as pl

# 常量定义
DEBUGGER_ADDRESS = "localhost:9222"
TARGET_URL = "http://10.188.34.1/cs/workbench/page/workbench.html?t=linestaging&layout=react"

@dataclass
class ProcessDataFrame:
    """工单处理时间数据结构"""
    start_time: Optional[datetime] = None  # 开始时间
    dispatch_time: Optional[datetime] = None  # 派发时间
    last_process_time: Optional[datetime] = None  # 最后处理时间
    final_review_time: Optional[datetime] = None  # 最终审核时间
    archive_time: Optional[datetime] = None  # 归档时间
    timeout_step: str = ""  # 超时环节
    timeout_issue: str = ""  # 超时问题定位

class WorkOrderProcessor:
    def __init__(self):
        self.driver = self._init_driver()

    def _init_driver(self):
        """初始化Edge浏览器驱动"""
        options = Options()
        options.add_experimental_option("debuggerAddress", DEBUGGER_ADDRESS)
        return webdriver.Edge(options=options)

    def process_work_order(self, row_data: dict) -> Optional[ProcessDataFrame]:
        """处理单个工单"""
        try:
            # 1. 准备工单查询页面
            if not self._prepare_query_page():
                return None

            # 2. 输入搜索条件
            if not self._input_search_criteria(row_data):
                return None

            # 3. 处理搜索结果
            return self._handle_search_results(row_data)

        except Exception as e:
            print(f"处理工单时出错: {str(e)}")
            return None

    def _prepare_query_page(self) -> bool:
        """准备工单查询页面"""
        try:
            # 切换到默认内容
            self.driver.switch_to.default_content()

            # 激活工单查询标签
            query_tab = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//td[@title='工单查询']"))
            )
            if 'tab_item2_selected' not in query_tab.get_attribute('class'):
                query_tab.click()
                time.sleep(1)

            # 切换到工单查询iframe
            query_frame = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "page_gg9902040500"))
            )
            self.driver.switch_to.frame(query_frame)
            return True

        except Exception as e:
            print(f"准备工单查询页面时出错: {str(e)}")
            return False

    def _input_search_criteria(self, row_data: dict) -> bool:
        """输入搜索条件"""
        try:
            sheet_code = str(row_data.get('工单流水号', ''))
            business_number = str(row_data.get('业务号码', ''))
            
            # 清空输入框
            self._clear_search_fields()
            
            # 设置搜索条件
            if sheet_code and sheet_code.strip():
                print(f"使用工单流水号搜索: {sheet_code}")
                sheet_code_input = self.driver.find_element(By.ID, "sheetCode")
                sheet_code_input.send_keys(sheet_code)
                return True
            elif business_number and business_number.strip():
                print(f"使用业务号码搜索: {business_number}")
                business_number_input = self.driver.find_element(By.ID, "businessNo")
                business_number_input.send_keys(business_number)
                return True
            
            return False

        except Exception as e:
            print(f"输入搜索条件时出错: {str(e)}")
            return False

    def _handle_search_results(self, row_data: dict) -> Optional[ProcessDataFrame]:
        """处理搜索结果"""
        try:
            # 获取工单列表
            links = WebDriverWait(self.driver, 10).until(
                EC.presence_of_all_elements_located(
                    (By.XPATH, "//a[contains(@onclick, 'datagrid.openNewDetail')]")
                )
            )
            print(f"找到 {len(links)} 个工单")

            # 处理每个工单
            for index, _ in enumerate(links):
                result = self._process_single_work_order(index, row_data)
                if result:
                    return result

            return None

        except Exception as e:
            print(f"处理搜索结果时出错: {str(e)}")
            return None

    def _process_single_work_order(self, index: int, row_data: dict) -> Optional[ProcessDataFrame]:
        """处理单个工单详情"""
        try:
            print(f"\n处理第 {index + 1} 个工单")
            
            # 点击工单链接
            self._click_work_order_link(index)
            
            # 获取工单类型并处理
            order_type = self._get_work_order_type()
            if order_type == "10010":
                return self._process_10010_work_order()
            else:
                return self._process_10015_work_order(row_data)

        except Exception as e:
            print(f"处理第{index+1}个工单时出错: {str(e)}")
            return None
        finally:
            self._close_current_tab()

    def _click_work_order_link(self, index: int) -> None:
        """点击工单链接"""
        script = f"datagrid.openNewDetail({index})"
        print(f"点击工单链接: {script}")
        self.driver.execute_script(script)
        time.sleep(1)

    def _get_work_order_type(self) -> str:
        """获取工单类型"""
        try:
            # 切换到新打开的工单详情iframe
            self.driver.switch_to.default_content()
            new_frame = WebDriverWait(self.driver, 10).until(
                lambda d: [f for f in d.find_elements(By.TAG_NAME, "iframe") 
                          if f.get_attribute("id").endswith("_new")][0]
            )
            frame_id = new_frame.get_attribute("id")
            print(f"尝试切换到iframe: {frame_id}")
            self.driver.switch_to.frame(new_frame)

            # 获取受理渠道
            channel = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//span[contains(text(),'受理渠道')]/following-sibling::span"))
            ).text
            print(f"找到受理渠道: {channel}")

            return "10010" if "联通投诉平台" in channel else "10015"

        except Exception as e:
            print(f"获取工单类型时出错: {str(e)}")
            return "10015"

    def _process_10010_work_order(self) -> Optional[ProcessDataFrame]:
        """处理10010工单"""
        try:
            # 点击处理过程信息标签
            process_tab = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//div[@role='tab']//span[contains(text(), '处理过程信息')]"))
            )
            process_tab.click()
            time.sleep(1)

            return self._extract_process_times()

        except Exception as e:
            print(f"处理10010工单时出错: {str(e)}")
            return None

    def _process_10015_work_order(self, row_data: dict) -> Optional[ProcessDataFrame]:
        """处理10015工单"""
        try:
            # 点击总部工单信息标签
            headquarters_tab = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//div[@data-node-key='10015relationsheet']"))
            )
            headquarters_tab.click()
            time.sleep(1)

            # 检查工单号是否匹配
            target_code = row_data.get('工单流水号', '')
            if self._check_headquarters_order_match(target_code):
                return self._process_10010_work_order()
            return None

        except Exception as e:
            print(f"处理10015工单时出错: {str(e)}")
            return None

    def _check_headquarters_order_match(self, target_code: str) -> bool:
        """检查总部工单号是否匹配"""
        try:
            links = WebDriverWait(self.driver, 10).until(
                EC.presence_of_all_elements_located((By.XPATH, "//a[contains(@onclick, 'openNewDetail')]"))
            )
            print(f"找到 {len(links)} 个工单链接")

            for link in links:
                sheet_code = link.text.strip()
                if "绿色通道-" in sheet_code:
                    sheet_code = sheet_code.replace("绿色通道-", "")
                print(f"检查工单流水号: {sheet_code}")
                
                if sheet_code == target_code:
                    return True

            print(f"未找到匹配的工单流水号: {target_code}")
            return False

        except Exception as e:
            print(f"检查总部工单号匹配时出错: {str(e)}")
            return False

    def _extract_process_times(self) -> ProcessDataFrame:
        """提取处理时间信息"""
        result = ProcessDataFrame()
        try:
            # 获取处理过程表格数据
            rows = self.driver.find_elements(By.XPATH, "//table[@class='process-table']//tr")
            
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 2:
                    action = cells[0].text.strip()
                    time_str = cells[1].text.strip()
                    
                    try:
                        time_value = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                        
                        if "创建" in action:
                            result.start_time = time_value
                        elif "派发" in action:
                            result.dispatch_time = time_value
                        elif "处理" in action:
                            result.last_process_time = time_value
                        elif "审核" in action:
                            result.final_review_time = time_value
                        elif "归档" in action:
                            result.archive_time = time_value
                    except ValueError:
                        continue

        except Exception as e:
            print(f"提取处理时间信息时出错: {str(e)}")
        
        return result

    def _clear_search_fields(self) -> None:
        """清空搜索字段"""
        try:
            self.driver.execute_script("""
                document.getElementById('sheetCode').value = '';
                document.getElementById('businessNo').value = '';
            """)
            print("已清空表单数据")
        except Exception as e:
            print(f"清空搜索字段时出错: {str(e)}")

    def _close_current_tab(self) -> None:
        """关闭当前工单详情标签页"""
        try:
            self.driver.switch_to.default_content()
            close_button = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//span[@class='close']"))
            )
            close_button.click()
            print("已关闭当前工单详情标签页")
        except Exception as e:
            print(f"关闭标签页时出错: {str(e)}")



def main():
    # 读取Excel文件
    file_manager = FileManager("WorkDocument\\网络质量报表超时分析")
    
    df = file_manager.read_excel(file_path="网络质量报表-Test.xlsx",sheet_name="Sheet_0")
    if df.is_empty():
        print("Excel文件为空或无法读取")
        return
    
    # 初始化工单处理器
    processor = WorkOrderProcessor()
    
    # 存储处理结果
    results = []
    
    # 处理每一行数据
    for index, row in df.iter_rows(named=True):
        print(f"\n处理记录: 工单流水号={row['工单流水号']}, 业务号码={row['业务号码']}")
        # 处理工单
        process_data = processor.process_work_order(row)
        
        # 保存结果
        if process_data:
            results.append({
                '工单流水号': row['工单流水号'],
                '业务号码': row['业务号码'],
                '开始时间': process_data.start_time,
                '派发时间': process_data.dispatch_time,
                '最后处理时间': process_data.last_process_time,
                '最终审核时间': process_data.final_review_time,
                '归档时间': process_data.archive_time,
                '超时环节': process_data.timeout_step,
                '超时问题': process_data.timeout_issue
            })
    
    # 保存结果到Excel
    if results:
        result_df = pl.DataFrame(results)
        file_manager.save_to_excel(result_df,"网络质量报表超时分析")
    else:
        print("\n未获取到任何处理结果")

if __name__ == "__main__":
    main()