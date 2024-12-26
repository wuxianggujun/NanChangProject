import time
import datetime as dt
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.edge.options import Options
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from tool.file.FileManager import FileManager
import polars as pl

# 常量定义 (可根据实际情况修改)
DEBUGGER_ADDRESS = "localhost:9222"
TARGET_URL = "http://10.188.34.1/cs/workbench/page/workbench.html?t=linestaging&layout=react"


def launch_edge_with_remote_debugging(debugger_address=DEBUGGER_ADDRESS):
    options = Options()
    options.add_experimental_option("debuggerAddress", debugger_address)
    driver = webdriver.Edge(options=options)
    return driver


def check_work_order_query_page(driver):
    try:
        # 检查"工单查询"标签页是否处于激活状态
        element = driver.find_element(By.XPATH, "//td[@title='工单查询' and contains(@class, 'tab_item2_selected')]")
        print("当前激活的页面是'工单查询'。")

        # 检查业务号码输入框是否可见
        input_box = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.ID, "businessNo"))  # 修改为 businessNo
        )
        print("业务号码输入框可见。")
        return True

    except Exception as e:
        tab_element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//td[@title='工单查询']"))
        )
        tab_element.click()
        return False




def get_new_detail_tabs(driver):
    """获取所有新工单详情标签页的ID"""
    try:
        # 切换回主文档
        driver.switch_to.default_content()

        # 等待标签页加载
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "tabContainer"))
        )

        # 查找所有包含"新工单详情"的标签页
        detail_tabs = driver.find_elements(
            By.XPATH,
            "//div[contains(@class, 'tabContainer')]//table[contains(@id, '_new')]"
        )

        # 获取标签页ID
        tab_ids = []
        for tab in detail_tabs:
            try:
                tab_id = tab.get_attribute('id')
                if tab_id:  # 确保ID不为空
                    print(f"Found tab with ID: {tab_id}")
                    tab_ids.append(tab_id)
            except Exception as e:
                print(f"Error getting tab ID: {e}")

        print(f"Found {len(tab_ids)} detail tabs")
        return tab_ids

    except Exception as e:
        print(f"Error in get_new_detail_tabs: {e}")
        return []


def switch_to_tab(driver, tab_id):
    """切换到指定标签页"""
    try:
        print(f"Switching to tab ID: {tab_id}")
        tab = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, tab_id))
        )
        driver.execute_script("arguments[0].click();", tab)
        time.sleep(1)
        return True
    except Exception as e:
        print(f"Error switching to tab: {e}")
        return False


def switch_to_iframe(driver, iframe_id):
    """切换到指定iframe"""
    try:
        print(f"Looking for iframe with ID: {iframe_id}")
        WebDriverWait(driver, 10).until(
            EC.frame_to_be_available_and_switch_to_it((By.ID, iframe_id))
        )
        print("Successfully switched to iframe")
        return True
    except Exception as e:
        print(f"Error switching to iframe: {e}")
        return False


def get_headquarters_order_count(driver):
    """获取总部工单信息数量"""
    try:
        headquarters_tab = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, 
                "//div[@data-node-key='10015relationsheet']//span[contains(text(), '总部工单信息')]/span"))
        )
        count = int(headquarters_tab.text)
        print(f"总部工单信息数量: {count}")
        return count
    except Exception as e:
        print(f"Error getting headquarters order count: {e}")
        return 0


def close_empty_tab(driver, tab_id):
    """关闭空工单标签页"""
    try:
        driver.switch_to.default_content()
        close_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, 
                f"//td[@title='新工单详情']/following-sibling::td//div[contains(@class, 'tab_close')]"))
        )
        driver.execute_script("arguments[0].click();", close_button)
        print(f"关闭了总部工单信息数量为0的标签页: {tab_id}")
        time.sleep(1)
        return True
    except Exception as e:
        print(f"Error closing empty tab: {e}")
        return False


def process_headquarters_orders(driver):
    """处理总部工单"""
    try:
        # 点击总部工单信息标签
        headquarters_tab = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//div[@data-node-key='10015relationsheet']"))
        )
        driver.execute_script("arguments[0].click();", headquarters_tab)
        time.sleep(1)

        # 获取工单列表
        work_orders = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.XPATH, 
                "//td[@class='ant-table-cell']//span[@title and @style='color: blue; cursor: pointer;']"))
        )
        
        if not work_orders:
            print("No work order links found")
            return False

        print(f"Found {len(work_orders)} work order links")
        return process_work_order(driver, work_orders[0])
    except Exception as e:
        print(f"Error processing headquarters orders: {e}")
        return False


def process_work_order(driver, work_order_link):
    """处理单个工单"""
    try:
        work_order_number = work_order_link.get_attribute('title')
        print(f"Processing work order: {work_order_number}")
        
        # 点击工单链接
        driver.execute_script("arguments[0].click();", work_order_link)
        print("Clicked work order link")
        
        # 切回主文档并处理弹窗
        return handle_work_order_dialog(driver)
    except Exception as e:
        print(f"Error processing work order: {e}")
        return False



@dataclass
class ProcessDataFrame:
    """工单处理时间数据结构"""
    start_time: Optional[datetime] = None          # 开始（主办）时间
    dispatch_time: Optional[datetime] = None       # 审核派发（主办）时间
    last_process_time: Optional[datetime] = None   # 最后一次核查处理（主办）时间
    final_review_time: Optional[datetime] = None   # 结果审核（主办）时间
    archive_time: Optional[datetime] = None        # 归档（主办）时间
    timeout_step: str = ""                        # 超时环节
    timeout_issue: str = ""                       # 超时工单问题定位


def handle_search_results(driver) -> ProcessDataFrame:
    """处理搜索结果并返回处理时间信息"""
    try:
        # 切换到默认内容
        driver.switch_to.default_content()
        
        # 确保工单查询标签页处于激活状态
        query_tab = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//td[@title='工单查询']"))
        )
        if 'tab_item2_selected' not in query_tab.get_attribute('class'):
            query_tab.click()
            time.sleep(1)

        # 切换到工单查询iframe并点击工单链接
        iframe = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "page_gg9902040500"))
        )
        driver.switch_to.frame(iframe)

        links = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located(
                (By.XPATH, "//a[contains(@onclick, 'datagrid.openNewDetail')]")
            )
        )

        if not links:
            print("未找到相关工单")
            return None

        # 处理第一个工单
        work_order = links[0]
        onclick = work_order.get_attribute('onclick')
        if onclick:
            driver.execute_script(onclick)
            time.sleep(2)  # 等待新页面加载
            
            # 切换到默认内容以查找新打开的iframe
            driver.switch_to.default_content()
            
            # 等待并获取新打开的iframe
            new_iframes = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.XPATH, "//iframe[contains(@id, '_new')]"))
            )
            
            for iframe in new_iframes:
                try:
                    iframe_id = iframe.get_attribute('id')
                    print(f"尝试切换到iframe: {iframe_id}")
                    
                    # 切换到新iframe
                    driver.switch_to.frame(iframe)
                    
                    # 使用更精确的XPath定位受理渠道
                    channel_xpath = "//span[contains(@class, 'ant-descriptions-item-label') and contains(text(), '受理渠道')]/following-sibling::span[contains(@class, 'ant-descriptions-item-content')]"
                    
                    channel_element = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, channel_xpath))
                    )
                    
                    channel_text = channel_element.text.strip()
                    print(f"找到受理渠道: {channel_text}")
                    
                    if "10010客服热线" in channel_text:
                        print("检测到10010工单，直接获取时间信息")
                        process_times = get_process_info_10010(driver)
                        driver.switch_to.default_content()
                        return process_times
                    else:
                        print(f"非10010工单，当前受理渠道: {channel_text}")
                        driver.switch_to.default_content()
                        
                except Exception as e:
                    print(f"在iframe {iframe_id} 中查找受理渠道时出错: {e}")
                    driver.switch_to.default_content()
                    continue
            
            # 如果没有找到10010工单或出现错误，处理弹窗
            return handle_work_order_dialog(driver)

        return None

    except Exception as e:
        print(f"处理搜索结果时出错: {e}")
        driver.switch_to.default_content()
        return None


def input_search_criteria(driver, support_code: str, sheet_code: str, business_number: str) -> bool:
    """输入搜索条件"""
    try:
        # 工单流水号输入框
        sheet_code_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "sheetCode"))
        )
        # 业务号码输入框
        business_number_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "businessNo"))
        )

        # 清空输入框
        if sheet_code_input.get_attribute("value"):
            sheet_code_input.clear()
        if business_number_input.get_attribute("value"):
            business_number_input.clear()

        # 判断是否使用业务号码查询
        if support_code in ['#N/A', '', None]:
            if business_number and business_number.strip():
                print(f"支撑系统工单号无效，使用业务号码查询: {business_number}")
                business_number_input.send_keys(business_number)
                return True
            else:
                print("无有效的业务号码")
                return False
        else:
            # 使用工单流水号查询
            if sheet_code and sheet_code.strip():
                print(f"使用工单流水号查询: {sheet_code}")
                sheet_code_input.send_keys(sheet_code)
                return True
            else:
                print("无有效的工单流水号")
                return False

    except Exception as e:
        print(f"输入搜索条件时出错: {e}")
        return False

def parse_sheet_code_date(sheet_code: str) -> Optional[dt.datetime]:
    """从工单流水号解析日期"""
    try:
        if not sheet_code or sheet_code == '#N/A':
            return None
            
        # 工单号格式：TS202410161712105399
        # 提取年月日: 20241016
        date_str = sheet_code[2:10]
        return dt.datetime.strptime(date_str, '%Y%m%d')
    except Exception as e:
        print(f"解析工单流水号日期失败: {e}")
        return None

def set_search_date_range(driver, sheet_code: str) -> bool:
    """根据工单流水号设置搜索时间范围"""
    try:
        sheet_date = parse_sheet_code_date(sheet_code)
        if not sheet_date:
            print(f"无法从工单流水号 {sheet_code} 解析日期")
            return False

        # 使用工单日期前一天作为开始时间
        start_date = sheet_date - dt.timedelta(days=1)
        end_date = start_date + dt.timedelta(days=30)
        print(f"设置查询时间范围: {start_date.date()} 到 {end_date.date()}")
        
        start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S')

        # 设置开始时间
        accept_time_begin = driver.find_element(By.ID, "acceptTimeBegin")
        accept_time_begin.clear()
        driver.execute_script(f"arguments[0].value = '{start_date_str}';", accept_time_begin)

        # 设置结束时间
        accept_time_end = driver.find_element(By.ID, "acceptTimeEnd")
        accept_time_end.clear()
        driver.execute_script(f"arguments[0].value = '{end_date_str}';", accept_time_end)
        
        return True

    except Exception as e:
        print(f"设置时间范围时出错: {e}")
        return False

def process_business_number(driver, row_data: dict, file_manager: FileManager) -> ProcessDataFrame:
    """处理单个业务记录"""
    try:
        # 获取支撑系统工单号、工单流水号和业务号码
        support_code = str(row_data.get('支撑系统工单号', ''))
        sheet_code = str(row_data.get('工单流水号', ''))
        business_number = str(row_data.get('业务号码', ''))

        # 输入搜索条件
        if not input_search_criteria(driver, support_code, sheet_code, business_number):
            return None

        # 无论使用哪种方式查询，都使用工单流水号的时间来设置范围
        if not set_search_date_range(driver, sheet_code):
            return None

        # 点击搜索按钮
        search_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(@onclick, 'datagrid.search()')]"))
        )
        search_button.click()
        time.sleep(1)  # 等待搜索结果加载

        # 处理搜索结果
        return handle_search_results(driver)

    except Exception as e:
        print(f"处理记录时出错: {e}")
        return None


def parse_datetime(time_str: str) -> datetime:
    """解析时间字符串为datetime对象"""
    try:
        return datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"Error parsing datetime: {e}")
        return None

def calculate_timeout_hours(start: Optional[datetime], end: Optional[datetime]) -> float:
    """计算两个时间点之间的小时差"""
    if not start or not end:
        return 0.0
    time_diff = end - start
    return time_diff.total_seconds() / 3600  # 转换为小时

def get_process_info(driver) -> ProcessDataFrame:
    """获取处理过程信息中的数据"""
    try:
        # 切换到处理过程信息标签
        process_tab = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, 
                "//div[@data-node-key='sheetprocess']"))
        )
        driver.execute_script("arguments[0].click();", process_tab)
        print("Switched to process info tab")
        time.sleep(1)

        # 初始化数据结构
        process_times = ProcessDataFrame()
        latest_process_time = None
        latest_review_time = None

        # 获取所有页面的数据
        while True:
            # 获取当前页面的所有行
            rows = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.XPATH, 
                    "//tr[@class='ant-table-row ant-table-row-level-0']"))
            )
            
            print(rows)

            for row in rows:
                # 获取环节和处理时间
                step = row.find_element(By.XPATH, ".//td[5]").text
                process_time_str = row.find_element(By.XPATH, ".//td[3]").text
                deadline_str = row.find_element(By.XPATH, ".//td[4]").text
                current_time = parse_datetime(process_time_str)
                deadline_time = parse_datetime(deadline_str)
                
                if not current_time or not deadline_time:
                    continue

                # 检查是否超时
                if current_time > deadline_time:
                    if not process_times.timeout_step:
                        process_times.timeout_step = step
                        # 获取超时原因（从展开行获取）
                        try:
                            expanded_row = row.find_element(By.XPATH, 
                                "following-sibling::tr[@class='ant-table-expanded-row ant-table-expanded-row-level-1']")
                            process_times.timeout_issue = expanded_row.find_element(By.XPATH, ".//p").text
                        except Exception as e:
                            print(f"Error getting timeout issue: {e}")

                # 根据环节类型保存时间
                if "开始（主办）" in step and not process_times.start_time:
                    process_times.start_time = current_time
                    print(f"Found start time: {process_time_str}")
                
                elif "审核派发（主办）" in step and not process_times.dispatch_time:
                    process_times.dispatch_time = current_time
                    print(f"Found dispatch time: {process_time_str}")
                
                elif "核查处理（主办）" in step:
                    # 更新为时间最大的一次
                    if not latest_process_time or current_time > latest_process_time:
                        latest_process_time = current_time
                        process_times.last_process_time = current_time
                        print(f"Updated process time: {process_time_str}")
                
                elif "结果审核（主办）" in step:
                    # 更新为时间最大的一次
                    if not latest_review_time or current_time > latest_review_time:
                        latest_review_time = current_time
                        process_times.final_review_time = current_time
                        print(f"Updated final review time: {process_time_str}")
                
                elif "归档（主办）" in step:
                    process_times.archive_time = current_time
                    print(f"Found archive time: {process_time_str}")

            # 检查是否有下一页
            next_button = driver.find_element(By.XPATH, 
                "//li[contains(@class, 'ant-pagination-next')]")
            
            if 'ant-pagination-disabled' in next_button.get_attribute('class'):
                break
            
            # 点击下一页
            driver.execute_script("arguments[0].click();", next_button)
            time.sleep(1)

        # 获取所有数据后，计算超时环节
        if process_times.start_time and process_times.dispatch_time:
            dispatch_hours = calculate_timeout_hours(process_times.start_time, process_times.dispatch_time)
            print(f"审核派发耗时: {dispatch_hours:.2f}小时")
            max_hours = dispatch_hours
            process_times.timeout_step = "审核派发"
            process_times.timeout_issue = "审核派发环节耗时过长"

        if process_times.dispatch_time and process_times.last_process_time:
            process_hours = calculate_timeout_hours(process_times.dispatch_time, process_times.last_process_time)
            print(f"核查处理耗时: {process_hours:.2f}小时")
            if process_hours > max_hours:
                max_hours = process_hours
                process_times.timeout_step = "核查处理"
                process_times.timeout_issue = "核查处理环节耗时过长"

        if process_times.last_process_time and process_times.final_review_time:
            review_hours = calculate_timeout_hours(process_times.last_process_time, process_times.final_review_time)
            print(f"结果审核耗时: {review_hours:.2f}小时")
            if review_hours > max_hours:
                max_hours = review_hours
                process_times.timeout_step = "结果审核"
                process_times.timeout_issue = "结果审核环节耗时过长"

        if process_times.final_review_time and process_times.archive_time:
            archive_hours = calculate_timeout_hours(process_times.final_review_time, process_times.archive_time)
            print(f"归档耗时: {archive_hours:.2f}小时")
            if archive_hours > max_hours:
                max_hours = archive_hours
                process_times.timeout_step = "回访工作"
                process_times.timeout_issue = "回访工作环节耗时过长"

        print(f"\n最长耗时环节: {process_times.timeout_step}, 耗时: {max_hours:.2f}小时")
        return process_times

    except Exception as e:
        print(f"Error getting process info: {e}")
        return ProcessDataFrame()


def handle_work_order_dialog(driver):
    """处理工单弹窗"""
    try:
        driver.switch_to.default_content()
        print("Waiting for dialog div to appear...")
        
        dialog_div = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "_DialogDiv_1fk"))
        )
        
        if not dialog_div.is_displayed():
            print("Dialog div is not visible")
            return False

        print("Dialog div is visible")
        
        # 切换到弹窗iframe
        dialog_frame = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "_DialogFrame_1fk"))
        )
        print("Dialog frame found")
        
        # 切换到iframe
        driver.switch_to.frame("_DialogFrame_1fk")
        print("Switched to dialog frame")

        # 获取处理时间
        process_times = get_process_info(driver)
        if process_times:
            print("\n处理时间信息:")
            print(f"开始时间: {process_times.start_time}")
            print(f"派发时间: {process_times.dispatch_time}")
            print(f"最后处理时间: {process_times.last_process_time}")
            print(f"最终审核时间: {process_times.final_review_time}")
            print(f"归档时间: {process_times.archive_time}")
            print(f"超时环节: {process_times.timeout_step}")
            print(f"超时问题: {process_times.timeout_issue}")
        
        # 切回主文档
        driver.switch_to.default_content()
        
        return close_dialog(driver)

    except Exception as e:
        print(f"Error handling work order dialog: {e}")
        driver.switch_to.default_content()
        return False


def close_dialog(driver):
    """关闭工单弹窗"""
    try:
        print("Attempting to close dialog...")
        driver.execute_script("""
            if (typeof fixProgress === 'function') {
                fixProgress();
            }
            if (Dialog && Dialog.getInstance('1fk')) {
                Dialog.getInstance('1fk').cancelButton.onclick.apply(
                    Dialog.getInstance('1fk').cancelButton,[]
                );
            }
        """)
        print("Dialog close command executed")
        
        # 等待弹窗消失
        WebDriverWait(driver, 10).until_not(
            EC.presence_of_element_located((By.ID, "_DialogDiv_1fk"))
        )
        print("Dialog confirmed closed")
        return True
    except Exception as e:
        print(f"Error closing dialog: {e}")
        return False


def process_detail_tabs(driver):
    """处理所有工单详情标签页"""
    try:
        time.sleep(2)
        tab_ids = get_new_detail_tabs(driver)
        processed_tabs = set()

        for tab_id in tab_ids:
            if tab_id in processed_tabs:
                continue

            if not switch_to_tab(driver, tab_id):
                continue

            iframe_id = f"page_{tab_id}"
            if not switch_to_iframe(driver, iframe_id):
                continue

            count = get_headquarters_order_count(driver)
            
            if count == 0:
                close_empty_tab(driver, tab_id)
            else:
                process_headquarters_orders(driver)

            processed_tabs.add(tab_id)

    except Exception as e:
        print(f"Error in process_detail_tabs: {e}")
        driver.switch_to.default_content()


def click_work_order_link(driver, link):
    """尝试在正确的上下文中点击链接"""
    try:
        # 1. 切换到默认内容
        driver.switch_to.default_content()

        # 2. 确保工单查询标签页处于激活状态
        query_tab = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//td[@title='工单查询']"))
        )
        if 'tab_item2_selected' not in query_tab.get_attribute('class'):
            print("Activating work order query tab")
            query_tab.click()
            time.sleep(1)  # 等待标签页切换

        # 3. 切换到工单查询iframe
        iframe = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "page_gg9902040500"))
        )
        driver.switch_to.frame(iframe)

        # 4. 重新获取链接元素（因为可能已经过期）
        link_xpath = f"//a[contains(@onclick, 'datagrid.openNewDetail') and text()='{link.text}']"
        new_link = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, link_xpath))
        )

        # 5. 获取并执行onclick事件
        onclick = new_link.get_attribute('onclick')
        if onclick:
            print(f"Attempting to execute onclick: {onclick}")
            driver.execute_script(onclick)
            time.sleep(1)  # 等待点击效果
            return True

        return False

    except Exception as e:
        print(f"Error in click_work_order_link: {e}")
        return False
    finally:
        # 确保切换回默认内容
        try:
            driver.switch_to.default_content()
        except:
            pass


def inspect_link(driver, link):
    """检查链接的属性和状态"""
    try:
        print("\n=== Link Inspection ===")
        print(f"Link Text: {link.text}")
        print(f"Link HTML: {link.get_attribute('outerHTML')}")
        print(f"onclick attribute: {link.get_attribute('onclick')}")
        print(f"href attribute: {link.get_attribute('href')}")
        print(f"Is Displayed: {link.is_displayed()}")
        print(f"Is Enabled: {link.is_enabled()}")
        print("=====================\n")
    except Exception as e:
        print(f"Error inspecting link: {e}")


def save_process_times_to_excel(process_times: ProcessDataFrame, file_manager: FileManager):
    """将处理时间信息保存到Excel文件"""
    try:
        # 创建数据字典
        data = {
            "开始时间": [process_times.start_time],
            "派发时间": [process_times.dispatch_time],
            "最后处理时间": [process_times.last_process_time],
            "最终审核时间": [process_times.final_review_time],
            "归档时间": [process_times.archive_time],
            "超时环节": [process_times.timeout_step],
            "超时问题": [process_times.timeout_issue]
        }
        
        # 转换为 Polars DataFrame
        df = pl.DataFrame(data)
        
        # 读取现有文件
        file_path = "WorkDocument/网络质量报表超时分析/网络质量报表-Test.xlsx"
        existing_df = file_manager.read_excel(file_path)
        
        # 如果现有文件有数据，则追加新数据
        if not existing_df.is_empty():
            df = pl.concat([existing_df, df])
        
        # 保存回文件
        file_manager.save_to_excel(df, "网络质量报表超时分析")
        print("数据已成功保存到Excel文件")

    except Exception as e:
        print(f"保存数据到Excel时出错: {e}")

def get_process_info_10010(driver) -> ProcessDataFrame:
    """获取10010工单处理过程信息中的数据"""
    try:
        # 切换到处理过程信息标签
        process_tab = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//div[@data-node-key='sheetprocess']"))
        )
        driver.execute_script("arguments[0].click();", process_tab)
        print("Switched to process info tab")
        time.sleep(1)

        # 初始化数据结构
        process_times = ProcessDataFrame()
        
        # 获取表格数据
        rows = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 
                ".ant-table-tbody tr:not(.ant-table-expanded-row)"))
        )

        # 从后向前遍历处理记录
        for row in reversed(rows):
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) >= 4:
                arrive_time_str = cells[0].text.strip()
                step = cells[3].text.strip()
                
                try:
                    arrive_time = datetime.strptime(arrive_time_str, '%Y-%m-%d %H:%M:%S')
                    
                    if step == "开始":
                        process_times.start_time = arrive_time
                    elif step == "区域派送":
                        process_times.dispatch_time = arrive_time
                    elif step == "核查处理":
                        process_times.last_process_time = arrive_time
                    elif step == "结果审核":
                        process_times.final_review_time = arrive_time
                    elif step == "归档":
                        process_times.archive_time = arrive_time
                        
                except ValueError as e:
                    print(f"Error parsing datetime: {e}")
                    continue

        # 计算各环节耗时并找出最长耗时环节
        max_hours = 0
        
        if process_times.start_time and process_times.dispatch_time:
            hours = calculate_timeout_hours(process_times.start_time, process_times.dispatch_time)
            if hours > max_hours:
                max_hours = hours
                process_times.timeout_step = "区域派送"
                process_times.timeout_issue = "区域派送环节耗时过长"

        if process_times.dispatch_time and process_times.last_process_time:
            hours = calculate_timeout_hours(process_times.dispatch_time, process_times.last_process_time)
            if hours > max_hours:
                max_hours = hours
                process_times.timeout_step = "核查处理"
                process_times.timeout_issue = "核查处理环节耗时过长"

        if process_times.last_process_time and process_times.final_review_time:
            hours = calculate_timeout_hours(process_times.last_process_time, process_times.final_review_time)
            if hours > max_hours:
                max_hours = hours
                process_times.timeout_step = "结果审核"
                process_times.timeout_issue = "结果审核环节耗时过长"

        print(f"\n最长耗时环节: {process_times.timeout_step}, 耗时: {max_hours:.2f}小时")
        return process_times

    except Exception as e:
        print(f"Error getting process info for 10010: {e}")
        return ProcessDataFrame()

def activate_work_order_query_tab(driver):
    """激活工单查询标签页并关闭其他标签，然后切换到主frame"""
    try:
        # 找到工单查询标签
        work_order_tab = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, 
                "//td[contains(@class, 'tab_title') and @title='工单查询']"))
        )
        
        # 右键点击标签
        actions = ActionChains(driver)
        actions.context_click(work_order_tab).perform()
        
        # 等待右键菜单出现并点击"关闭其他标签"
        close_others = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, 
                "//div[contains(@class, 'l-menu-item-text') and text()='关闭其他标签']"))
        )
        close_others.click()
        
        # 确保工单查询标签被激活
        if 'tab_item2_selected' not in work_order_tab.get_attribute('class'):
            work_order_tab.click()
            
        print("工单查询标签已激活，其他标签已关闭")

        # 切换到主frame
        driver.switch_to.default_content()  # 先切回默认内容
        main_iframe = WebDriverWait(driver, 10).until(
            EC.frame_to_be_available_and_switch_to_it((By.ID, "page_gg9902040500"))
        )
        print("已切换到工单查询主frame")
        
        return True

    except Exception as e:
        print(f"激活工单查询标签时出错: {e}")
        return False

def main():
    file_manager = FileManager(".")
    try:
        # 读取Excel文件
        file_path = "WorkDocument/网络质量报表超时分析/网络质量报表-Test.xlsx"
        df = file_manager.read_excel(file_path,sheet_name="Sheet_0")
        
        if df.is_empty():
            print("Excel文件为空或无法读取")
            return

        driver = launch_edge_with_remote_debugging(DEBUGGER_ADDRESS)
        # input("请确保已在 Edge 浏览器中打开目标页面并登录，然后按回车继续执行自动化操作...")

        check_work_order_query_page(driver)

        # 切换到主iframe
        main_iframe = WebDriverWait(driver, 10).until(
            EC.frame_to_be_available_and_switch_to_it((By.ID, "page_gg9902040500"))
        )
        
        # 逐行处理数据
        results = []
        for row in df.iter_rows(named=True):
            row_dict = dict(row)
            print(f"\n处理记录: 工单流水号={row_dict.get('工单流水号', 'N/A')}, 业务号码={row_dict.get('业务号码', 'N/A')}")
            
            # 处理单个记录
            process_times = process_business_number(driver, row_dict, file_manager)
            
            if process_times:
                # 将处理时间信息添加到原始数据中
                row_dict.update({
                    "开始时间": process_times.start_time,
                    "派发时间": process_times.dispatch_time,
                    "最后处理时间": process_times.last_process_time,
                    "最终审核时间": process_times.final_review_time,
                    "归档时间": process_times.archive_time,
                    "超时环节": process_times.timeout_step,
                    "超时问题": process_times.timeout_issue
                })
            results.append(row_dict)
            # 在开始新查询前调用
            activate_work_order_query_tab(driver)
            # 保存所有结果
        if results:
            result_df = pl.DataFrame(results)
            file_manager.save_to_excel(result_df, "网络质量报表超时分析")
            print("所有数据处理完成并保存")

    except Exception as e:
        print(f"发生错误: {e}")
        driver.switch_to.default_content()


if __name__ == "__main__":
    main()
