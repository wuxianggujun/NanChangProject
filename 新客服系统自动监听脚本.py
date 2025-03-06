#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
微信自动监听脚本
功能：监控微信窗口，获取会话列表信息，监控有无新消息
"""

import uiautomation as auto
import time
import logging
import os
import re
from datetime import datetime
import win32gui
import win32con
import random

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('wechat_monitor.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 微信配置
class WeChatConfig:
    PROCESS_NAME = 'WeChat.exe'
    WINDOW_NAME = '微信'
    WINDOW_CLASSNAME = 'WeChatMainWndForPC'
    # 最大会话数量
    MAX_CONVERSATIONS = 30
    # 排除的会话名称关键词
    EXCLUDE_KEYWORDS = ['公众号', '订阅号', '微信团队', '折叠置顶', '服务通知']

class WeChatMonitor:
    def __init__(self):
        """初始化微信监控器"""
        self.wechat_window = None
        # 用户名（用于检测@消息）
        self.username = "我"  # 默认为"我"，可根据实际情况修改
        # 会话列表控件
        self.conversation_list = None
        # 调试模式
        self.debug = True
        # 会话数据
        self.conversations = []
        # 当前会话列表的所有项目
        self.all_conversation_items = []
        # 最大滚动尝试次数
        self.max_scroll_attempts = 30
        # 自动点击开关
        self.auto_click_enabled = False
        # 已处理会话ID
        self.processed_ids = set()
        
    def wake_up_window(self):
        """激活微信窗口，使用win32gui方法"""
        try:
            hwnd = win32gui.FindWindow(WeChatConfig.WINDOW_CLASSNAME, WeChatConfig.WINDOW_NAME)
            if hwnd:
                # 恢复窗口
                win32gui.ShowWindow(hwnd, win32con.SW_RESTORE)
                # 尝试将窗口置前
                try:
                    win32gui.SetForegroundWindow(hwnd)
                    logger.info("已成功激活微信窗口")
                    time.sleep(1)  # 等待窗口激活
                    return True
                except Exception as e:
                    logger.error(f"尝试将窗口置前时出错: {e}")
                    return False
            else:
                logger.warning("未找到微信窗口")
                return False
        except Exception as e:
            logger.error(f"激活微信窗口时出错: {e}")
            return False
    
    def find_wechat_window(self):
        """查找并获取微信主窗口"""
        try:
            # 先尝试使用win32gui激活窗口
            if not self.wake_up_window():
                logger.warning("无法激活微信窗口，尝试直接查找")
            
            # 尝试查找微信主窗口
            wechat_window = auto.WindowControl(ClassName=WeChatConfig.WINDOW_CLASSNAME)
            if wechat_window.Exists(3):
                logger.info("找到微信窗口")
                
                # 再次尝试激活窗口
                try:
                    wechat_window.SetActive()
                    time.sleep(1)
                    logger.info("通过UIAutomation激活了微信窗口")
                except Exception as e:
                    logger.warning(f"通过UIAutomation激活窗口失败: {e}")
                
                self.wechat_window = wechat_window
                return True
            else:
                logger.warning("未找到微信窗口，请确保微信已启动")
                return False
        except Exception as e:
            logger.error(f"查找微信窗口时出错: {e}")
            return False
    
    def print_control_info(self, control, prefix=""):
        """打印控件信息"""
        try:
            logger.info(f"{prefix}控件: Name='{control.Name}', ClassName='{control.ClassName}', ControlType='{control.ControlTypeName}'")
            
            try:
                children = control.GetChildren()
                for i, child in enumerate(children):
                    try:
                        self.print_control_info(child, prefix=f"{prefix}    {i+1}. ")
                    except:
                        logger.info(f"{prefix}    {i+1}. [无法获取子控件信息]")
            except Exception as e:
                logger.debug(f"{prefix}[无法获取子控件: {e}]")
                
        except Exception as e:
            logger.error(f"打印控件信息出错: {e}")
    
    def find_conversation_list(self):
        """查找会话列表控件"""
        if not self.wechat_window:
            return None
        
        try:
            # 直接通过Name查找会话列表
            conv_list = self.wechat_window.ListControl(Name="会话")
            if conv_list.Exists(1):
                children_count = len(conv_list.GetChildren())
                logger.info(f"找到会话列表 'Name=会话', 包含 {children_count} 个可见会话")
                self.conversation_list = conv_list
                return conv_list
                
            # 尝试通过其他方式查找
            all_lists = self.wechat_window.GetChildren()
            for item in all_lists:
                if item.ControlTypeName == "ListControl":
                    children = item.GetChildren()
                    if len(children) > 0:
                        logger.info(f"找到可能的会话列表: Name='{item.Name}', 包含 {len(children)} 个可见项目")
                        self.conversation_list = item
                        return item
            
            logger.warning("未找到会话列表控件")
            return None
        except Exception as e:
            logger.error(f"查找会话列表时出错: {e}")
            return None
    
    def check_conversation_unread(self, conversation_item):
        """检查会话是否有未读消息"""
        try:
            # 方法1: 检查是否有未读计数
            if hasattr(conversation_item, "Name") and conversation_item.Name:
                if re.search(r"\(\d+\)", conversation_item.Name):
                    return True
                    
            # 方法2: 检查是否有特殊标记
            children = conversation_item.GetChildren()
            for child in children:
                try:
                    # 检查是否有红点标记（通常是小的图片控件）
                    if (child.ControlTypeName == "ImageControl" or 
                        child.ControlTypeName == "TextControl"):
                        # 红点通常很小，且名称可能是空或特殊值
                        if hasattr(child, "BoundingRectangle"):
                            try:
                                rect = child.BoundingRectangle
                                # 尝试不同的方式获取宽高
                                if hasattr(rect, 'width') and hasattr(rect, 'height'):
                                    width = rect.width
                                    height = rect.height
                                else:
                                    width = rect.right - rect.left
                                    height = rect.bottom - rect.top
                                
                                # 红点通常很小
                                if width < 20 and height < 20 and width > 0 and height > 0:
                                    return True
                            except:
                                pass
                except:
                    continue
                    
            return False
        except Exception as e:
            logger.error(f"检查会话未读状态时出错: {e}")
            return False
    
    def generate_conversation_id(self, conversation_item):
        """为会话项生成唯一ID，用于跟踪已处理的会话"""
        try:
            # 优先使用名称
            name = conversation_item.Name if hasattr(conversation_item, "Name") else ""
            if name:
                return name
                
            # 如果没有名称，使用边界矩形
            if hasattr(conversation_item, "BoundingRectangle"):
                rect = conversation_item.BoundingRectangle
                if hasattr(rect, 'left') and hasattr(rect, 'top'):
                    return f"item_{rect.left}_{rect.top}"
                    
            # 最后使用对象ID
            return str(id(conversation_item))
        except:
            return str(id(conversation_item))
    
    def should_exclude_conversation(self, name):
        """检查会话是否应该被排除"""
        # 名称为空的会话不排除
        if not name:
            return False
            
        # 检查是否包含排除关键词
        for keyword in WeChatConfig.EXCLUDE_KEYWORDS:
            if keyword in name:
                logger.info(f"排除会话: {name} (包含关键词: {keyword})")
                return True
                
        return False
    
    def scroll_conversation_list(self, direction="down"):
        """滚动会话列表
        
        Args:
            direction: 滚动方向，"up"向上，"down"向下
        
        Returns:
            bool: 是否成功滚动
        """
        if not self.conversation_list:
            logger.warning("找不到会话列表，无法滚动")
            return False
            
        try:
            # 获取滚动前的会话项
            before_items = [item.Name for item in self.conversation_list.GetChildren() if hasattr(item, "Name")]
            
            # 用简单可靠的方法滚动
            try:
                # 确保控件有焦点
                self.conversation_list.SetFocus()
                time.sleep(0.2)
                
                # 选择滚动方向
                if direction == "down":
                    # 尝试滚轮方法
                    self.conversation_list.WheelDown(wheelTimes=3)
                else:
                    # 向上滚动
                    self.conversation_list.WheelUp(wheelTimes=3)
                
                time.sleep(0.5)  # 等待滚动生效
            except Exception as e:
                logger.warning(f"使用内置滚轮方法失败: {e}，尝试键盘方法")
                
                try:
                    # 使用键盘滚动
                    self.conversation_list.SetFocus()
                    time.sleep(0.2)
                    
                    if direction == "down":
                        for _ in range(5):
                            auto.SendKeys('{Down}')
                            time.sleep(0.1)
                    else:
                        for _ in range(5):
                            auto.SendKeys('{Up}')
                            time.sleep(0.1)
                except Exception as e:
                    logger.error(f"使用键盘方法滚动失败: {e}")
                    return False
            
            # 检查滚动后的会话项
            time.sleep(0.5)
            after_items = [item.Name for item in self.conversation_list.GetChildren() if hasattr(item, "Name")]
            
            # 检查是否有变化
            if before_items == after_items:
                logger.info(f"滚动{direction}后会话列表没有变化")
                return False
            else:
                new_count = len(set(after_items) - set(before_items))
                logger.info(f"滚动{direction}后会话列表有 {new_count} 个新会话")
                return True
                
        except Exception as e:
            logger.error(f"滚动会话列表时出错: {e}")
            return False
    
    def click_conversation(self, conversation_item):
        """点击指定的会话项
        
        Args:
            conversation_item: 要点击的会话控件
            
        Returns:
            bool: 是否成功点击
        """
        if not self.auto_click_enabled:
            logger.info("自动点击功能已禁用，跳过点击操作")
            return False
            
        if not conversation_item:
            logger.warning("无效的会话项，无法点击")
            return False
            
        try:
            name = conversation_item.Name if hasattr(conversation_item, "Name") else "未知会话"
            logger.info(f"尝试点击会话: {name}")
            
            # 使用控件自带的点击方法
            try:
                conversation_item.Click(simulateMove=False)
                logger.info(f"已点击会话: {name}")
                time.sleep(0.5)  # 等待界面响应
                return True
            except Exception as e:
                logger.warning(f"直接点击会话控件失败: {e}，尝试其他方法")
                
                try:
                    # 尝试获取中心点坐标点击
                    rect = conversation_item.BoundingRectangle
                    center_x = int(rect.left + (rect.right - rect.left) * 0.5)
                    center_y = int(rect.top + (rect.bottom - rect.top) * 0.5)
                    
                    # 设置点击点，避开可能的未读标记
                    click_x = center_x
                    click_y = center_y
                    
                    # 确保窗口活动
                    self.wechat_window.SetActive()
                    time.sleep(0.2)
                    
                    # 使用坐标点击
                    auto.Click(click_x, click_y)
                    logger.info(f"已通过坐标点击会话: {name} 在位置 ({click_x}, {click_y})")
                    time.sleep(0.5)
                    return True
                except Exception as e:
                    logger.error(f"通过坐标点击会话失败: {e}")
                    return False
        except Exception as e:
            logger.error(f"点击会话过程中出错: {e}")
            return False
    
    def get_conversation_messages(self, conversation=None):
        """获取指定会话的消息内容（如果已打开）
        
        Args:
            conversation: 会话信息字典，可选
            
        Returns:
            list: 消息内容列表
        """
        try:
            # 查找可能的聊天窗口
            chat_area = None
            for pane in self.wechat_window.GetChildren():
                if pane.ControlTypeName == "PaneControl":
                    # 检查是否有消息列表
                    for child in pane.GetChildren():
                        if child.ControlTypeName == "ListControl" and child.Name != "会话":
                            chat_area = pane
                            break
                    if chat_area:
                        break
            
            if not chat_area:
                logger.info("未找到聊天窗口")
                return []
                
            # 查找消息列表
            message_list = None
            for control in chat_area.GetChildren():
                if control.ControlTypeName == "ListControl":
                    message_list = control
                    break
                    
            if not message_list:
                logger.info("未找到消息列表控件")
                return []
                
            # 获取最新消息
            messages = []
            message_items = message_list.GetChildren()
            
            # 获取最新的几条消息
            recent_count = min(5, len(message_items))
            if recent_count == 0:
                return []
                
            recent_message_items = message_items[-recent_count:]
            
            # 记录消息内容
            for i, msg in enumerate(recent_message_items):
                try:
                    message_text = msg.Name
                    if message_text:
                        messages.append(message_text)
                        
                        # 检查是否有@我的消息
                        if conversation and f"@{self.username}" in message_text:
                            sender_match = re.search(r"^(.*?):", message_text)
                            sender = sender_match.group(1) if sender_match else "未知发送者"
                            
                            logger.info(f"[通知] 检测到@消息: {sender} 在 {conversation.get('name', '未知会话')} 中@了你")
                            logger.info(f"[通知] 消息全文: {message_text}")
                except Exception as e:
                    logger.error(f"获取消息内容时出错: {e}")
            
            return messages
        except Exception as e:
            logger.error(f"获取会话消息时出错: {e}")
            return []
    
    def get_initial_conversations(self):
        """获取初始的会话列表（仅获取前30个非排除会话）"""
        if not self.find_conversation_list():
            logger.error("无法获取会话列表")
            return []
            
        try:
            logger.info("开始获取初始会话列表...")
            
            # 清空之前的会话数据
            self.conversations = []
            self.all_conversation_items = []
            valid_conversations = 0
            
            # 滚动到顶部
            logger.info("尝试滚动到列表顶部...")
            for _ in range(10):
                if not self.scroll_conversation_list("up"):
                    break
                time.sleep(0.2)
            
            logger.info("从顶部开始获取会话...")
            scroll_attempts = 0
            
            # 滚动并获取会话，直到达到所需数量或无法继续滚动
            while valid_conversations < WeChatConfig.MAX_CONVERSATIONS and scroll_attempts < self.max_scroll_attempts:
                # 获取当前可见的会话项
                current_items = self.conversation_list.GetChildren()
                if not current_items:
                    logger.warning("当前没有可见的会话项")
                    break
                    
                logger.info(f"当前可见会话数: {len(current_items)}")
                
                # 处理当前可见的会话项
                for item in current_items:
                    try:
                        # 获取会话名称
                        name = item.Name if hasattr(item, "Name") else f"会话 {len(self.conversations)}"
                        
                        # 生成会话ID
                        conv_id = self.generate_conversation_id(item)
                        
                        # 如果已处理过该会话或应该被排除，则跳过
                        if conv_id in self.processed_ids or self.should_exclude_conversation(name):
                            continue
                            
                        # 标记为已处理
                        self.processed_ids.add(conv_id)
                        
                        # 检查是否有未读标记
                        has_unread = self.check_conversation_unread(item)
                        unread_status = "【有未读】" if has_unread else ""
                        
                        # 添加到会话列表
                        conversation_info = {
                            "index": len(self.conversations),
                            "name": name,
                            "id": conv_id,
                            "has_unread": has_unread,
                            "item": item  # 存储控件引用
                        }
                        
                        self.conversations.append(conversation_info)
                        self.all_conversation_items.append(item)
                        
                        logger.info(f"{len(self.conversations)}. {name} {unread_status}")
                        
                        # 计数有效会话
                        valid_conversations += 1
                        
                        # 如果已达到目标数量，结束处理
                        if valid_conversations >= WeChatConfig.MAX_CONVERSATIONS:
                            break
                            
                    except Exception as e:
                        logger.error(f"处理会话项时出错: {e}")
                
                # 如果已达到目标数量，结束处理
                if valid_conversations >= WeChatConfig.MAX_CONVERSATIONS:
                    break
                    
                # 尝试向下滚动获取更多会话
                if not self.scroll_conversation_list("down"):
                    logger.info("无法再向下滚动，可能已到达列表底部")
                    scroll_attempts += 1
                    
                    if scroll_attempts >= 3:
                        logger.info("多次尝试滚动无效，停止获取")
                        break
                else:
                    # 成功滚动，重置尝试计数
                    scroll_attempts = 0
                
                # 短暂暂停，避免滚动过快
                time.sleep(0.2)
            
            logger.info(f"==== 初始会话列表获取完成 (共 {len(self.conversations)} 个) ====")
            return self.conversations
            
        except Exception as e:
            logger.error(f"获取初始会话列表时出错: {e}")
            return []
    
    def check_conversation_updates(self):
        """检查所有已获取会话的更新状态"""
        if not self.conversations:
            logger.warning("没有会话可检查")
            return
            
        try:
            logger.info("开始检查会话更新状态...")
            
            # 确保会话列表存在
            if not self.conversation_list or not self.conversation_list.Exists():
                if not self.find_conversation_list():
                    logger.error("无法找到会话列表")
                    return
            
            # 滚动到顶部
            logger.info("尝试滚动到列表顶部...")
            for _ in range(5):
                if not self.scroll_conversation_list("up"):
                    break
                time.sleep(0.2)
            
            # 获取当前可见的会话项
            current_visible = {}
            for item in self.conversation_list.GetChildren():
                if hasattr(item, "Name") and item.Name:
                    current_visible[item.Name] = item
            
            # 检查已知会话是否在当前可见列表中
            checked_count = 0
            unread_count = 0
            
            # 先检查可见的会话
            for conv in self.conversations:
                if checked_count >= 5:  # 限制每次检查的会话数
                    break
                    
                # 获取会话名称
                name = conv.get("name", "")
                if not name:
                    continue
                
                # 如果会话在当前可见列表中
                if name in current_visible:
                    item = current_visible[name]
                    
                    # 检查是否有未读
                    has_unread = self.check_conversation_unread(item)
                    if has_unread:
                        unread_count += 1
                        logger.info(f"[通知] 检测到未读会话: {name}")
                        
                        # 如果启用了自动点击，点击会话查看内容
                        if self.auto_click_enabled:
                            if self.click_conversation(item):
                                messages = self.get_conversation_messages(conv)
                                if messages:
                                    logger.info(f"==== {name} 的最新消息 ====")
                                    for i, msg in enumerate(messages):
                                        logger.info(f"  {i+1}. {msg}")
                    
                    checked_count += 1
            
            # 如果需要，向下滚动检查更多会话
            if checked_count < len(self.conversations):
                scroll_attempts = 0
                while checked_count < len(self.conversations) and scroll_attempts < 10:
                    # 滚动显示更多会话
                    if not self.scroll_conversation_list("down"):
                        scroll_attempts += 1
                        if scroll_attempts >= 3:
                            break
                    else:
                        scroll_attempts = 0
                    
                    # 获取新可见会话
                    current_visible = {}
                    for item in self.conversation_list.GetChildren():
                        if hasattr(item, "Name") and item.Name:
                            current_visible[item.Name] = item
                    
                    # 继续检查剩余会话
                    for conv in self.conversations[checked_count:]:
                        if checked_count >= len(self.conversations):
                            break
                            
                        name = conv.get("name", "")
                        if not name or name not in current_visible:
                            continue
                        
                        item = current_visible[name]
                        has_unread = self.check_conversation_unread(item)
                        if has_unread:
                            unread_count += 1
                            logger.info(f"[通知] 检测到未读会话: {name}")
                            
                            # 如果启用了自动点击，点击会话查看内容
                            if self.auto_click_enabled:
                                if self.click_conversation(item):
                                    messages = self.get_conversation_messages(conv)
                                    if messages:
                                        logger.info(f"==== {name} 的最新消息 ====")
                                        for i, msg in enumerate(messages):
                                            logger.info(f"  {i+1}. {msg}")
                        
                        checked_count += 1
                        if checked_count % 5 == 0:  # 每检查5个会话后暂停
                            break
                    
                    time.sleep(0.2)
            
            logger.info(f"会话检查完成: 检查了 {checked_count} 个会话，发现 {unread_count} 个有未读消息")
            
        except Exception as e:
            logger.error(f"检查会话更新状态时出错: {e}")
    
    def monitor(self, check_interval=5):
        """开始监控微信会话列表
        
        Args:
            check_interval: 检查间隔，单位为秒
        """
        logger.info("开始监控微信会话列表...")
        
        # 初始计数器，用于定期输出心跳信息
        heartbeat_counter = 0
        
        # 首先获取初始会话列表
        self.get_initial_conversations()
        
        while True:
            try:
                # 确保微信窗口存在
                if not self.wechat_window or not self.wechat_window.Exists():
                    if not self.find_wechat_window():
                        logger.warning("微信窗口不存在，等待下一次检查...")
                        time.sleep(check_interval)
                        continue
                
                # 心跳信息，让用户知道脚本在运行
                heartbeat_counter += 1
                if heartbeat_counter % 12 == 0:  # 大约每分钟输出一次
                    logger.info("监控脚本正在运行中...")
                
                # 检查会话更新状态
                self.check_conversation_updates()
                
                # 等待下一次检查
                time.sleep(check_interval)
                
            except Exception as e:
                logger.error(f"监控过程中出错: {e}")
                time.sleep(check_interval)
            except KeyboardInterrupt:
                logger.info("用户中断，退出监控")
                break


if __name__ == "__main__":
    try:
        logger.info("启动微信监控脚本")
        monitor = WeChatMonitor()
        # 可以在这里设置用户名
        monitor.username = "Do you want to continue ?"
        
        # 是否启用自动点击功能（默认禁用）
        monitor.auto_click_enabled = False
        
        # 尝试查找微信窗口
        if monitor.find_wechat_window():
            # 开始监控，每5秒检查一次
            monitor.monitor(check_interval=5)
        else:
            logger.error("无法找到微信窗口，请确保微信已经登录并正在运行")
    except KeyboardInterrupt:
        logger.info("用户中断，退出监控")
    except Exception as e:
        logger.error(f"程序运行出错: {e}")
