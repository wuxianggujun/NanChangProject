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
        # 检查“工单查询”标签页是否处于激活状态
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


def process_detail_tabs(driver):
    """处理所有工单详情标签页"""
    try:
        # 等待新标签页加载
        time.sleep(2)

        # 获取所有新工单详情标签页ID
        tab_ids = get_new_detail_tabs(driver)
        processed_tabs = set()  # 用于记录已处理的标签页
        print(f"Found {len(tab_ids)} detail tabs")

        for tab_id in tab_ids:
            if tab_id in processed_tabs:
                print(f"Tab {tab_id} already processed, skipping...")
                continue

            try:
                print(f"Processing tab ID: {tab_id}")

                # 先点击对应的标签页使其激活
                tab = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, tab_id))
                )
                driver.execute_script("arguments[0].click();", tab)
                time.sleep(1)  # 等待标签页切换完成

                # 构造iframe ID
                iframe_id = f"page_{tab_id}"
                print(f"Looking for iframe with ID: {iframe_id}")

                # 等待iframe出现并切换
                iframe = WebDriverWait(driver, 10).until(
                    EC.frame_to_be_available_and_switch_to_it((By.ID, iframe_id))
                )
                print("Successfully switched to iframe")

                # 在这里添加你需要在iframe中执行的操作
                # 例如：获取工单详情信息
                try:
                    # 这里添加获取工单信息的代码
                    pass
                except Exception as e:
                    print(f"Error getting work order details: {e}")
                # 切换回主文档
                driver.switch_to.default_content()
                # 标记该标签页已处理
                processed_tabs.add(tab_id)

            except Exception as e:
                print(f"Error processing tab {tab_id}: {e}")
                driver.switch_to.default_content()
                continue

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
