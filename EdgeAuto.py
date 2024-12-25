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
class ProcessTimes:
    """工单处理时间数据结构"""
    start_time: Optional[datetime] = None          # 开始（主办）时间
    dispatch_time: Optional[datetime] = None       # 审核派发（主办）时间
    last_process_time: Optional[datetime] = None   # 最后一次核查处理（主办）时间
    final_review_time: Optional[datetime] = None   # 结果审核（主办）时间
    archive_time: Optional[datetime] = None        # 归档（主办）时间
    timeout_step: str = ""                        # 超时环节
    timeout_issue: str = ""                       # 超时工单问题定位

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

def get_process_info(driver) -> ProcessTimes:
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
        process_times = ProcessTimes()
        latest_process_time = None
        latest_review_time = None

        # 获取所有页面的数据
        while True:
            # 获取当前页面的所有行
            rows = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.XPATH, 
                    "//tr[@class='ant-table-row ant-table-row-level-0']"))
            )

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
        return ProcessTimes()


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


def main():
    global driver
    try:
        driver = launch_edge_with_remote_debugging(DEBUGGER_ADDRESS)
        # input("请确保已在 Edge 浏览器中打开目标页面并登录，然后按回车继续执行自动化操作...")

        check_work_order_query_page(driver)

        # 切换到主iframe
        main_iframe = WebDriverWait(driver, 10).until(
            EC.frame_to_be_available_and_switch_to_it((By.ID, "page_gg9902040500"))
        )

        # 等待并清空输入框
        input_box = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "businessNo"))
        )

        if input_box.get_attribute("value"):
            print("业务号码输入框已有内容，清空输入框。")
            input_box.clear()
        input_box.send_keys("13226076050")

        # 设置生成时间：从 10月21号到后面共30天
        start_date = dt.datetime(2024, 10, 21, 0, 0, 0)  # 设置为 2023-10-21 00:00:00
        end_date = start_date + dt.timedelta(days=30)

        # 格式化日期时间为字符串，包含时分秒
        start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S')

        # 输入生成时间
        accept_time_begin = driver.find_element(By.ID, "acceptTimeBegin")
        accept_time_begin.clear()
        driver.execute_script(f"arguments[0].value = '{start_date_str}';", accept_time_begin)

        # 输入结束时间
        accept_time_end = driver.find_element(By.ID, "acceptTimeEnd")
        accept_time_end.clear()
        driver.execute_script(f"arguments[0].value = '{end_date_str}';", accept_time_end)

        # 后续操作直接查找元素，不需要等待
        search_button = driver.find_element(By.XPATH, "//button[contains(@onclick, 'datagrid.search()')]")
        search_button.click()

        # 获取所有工单链接
        driver.switch_to.default_content()
        iframe = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "page_gg9902040500"))
        )
        driver.switch_to.frame(iframe)

        # 等待列表加载
        rows = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.XPATH, "//tbody/tr"))
        )

        if not rows:
            print("列表为空，没有数据。")
            return

            # 获取所有工单链接
        links = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located(
                (By.XPATH, "//a[contains(@onclick, 'datagrid.openNewDetail')]")
            )
        )

        # 存储所有要处理的工单信息
        work_orders = []
        for link in links:
            work_order_number = link.text.strip()
            if work_order_number:
                work_orders.append({
                    'number': work_order_number,
                    'onclick': link.get_attribute('onclick')
                })

            print(f"Found {len(work_orders)} work orders to process")

            # 处理每个工单
        for index, work_order in enumerate(work_orders):
            try:
                print(f"\nProcessing work order {index + 1}: {work_order['number']}")

                # 切换到工单查询标签页
                driver.switch_to.default_content()
                query_tab = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//td[@title='工单查询']"))
                )
                if 'tab_item2_selected' not in query_tab.get_attribute('class'):
                    query_tab.click()
                    time.sleep(1)

                # 切换到iframe
                iframe = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, "page_gg9902040500"))
                )
                driver.switch_to.frame(iframe)

                # 记录当前标签页数量
                driver.switch_to.default_content()
                initial_tabs = len(get_new_detail_tabs(driver))

                # 切换回iframe并执行点击
                driver.switch_to.frame(iframe)
                print(f"Executing onclick: {work_order['onclick']}")
                driver.execute_script(work_order['onclick'])
                time.sleep(1)

                # 等待新标签页打开
                max_attempts = 10
                attempts = 0
                while attempts < max_attempts:
                    driver.switch_to.default_content()
                    current_tabs = len(get_new_detail_tabs(driver))
                    print(f"Current tabs: {current_tabs}, Initial tabs: {initial_tabs}")
                    if current_tabs > initial_tabs:
                        print(f"New tab opened successfully for work order {work_order['number']}")
                        break
                    time.sleep(1)
                    attempts += 1

                if attempts == max_attempts:
                    print(f"Failed to detect new tab for work order {work_order['number']}")
                    continue

                # 处理新打开的工单详情
                process_detail_tabs(driver)
                time.sleep(2)

            except Exception as e:
                print(f"Error processing work order {index + 1}: {e}")
                driver.switch_to.default_content()
                continue

    except Exception as e:
        print(f"发生错误: {e}")
        driver.switch_to.default_content()


if __name__ == "__main__":
    main()
