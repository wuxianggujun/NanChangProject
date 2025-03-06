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
from collections import OrderedDict
import notification
import json

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
        # 会话数据 - 使用OrderedDict保持顺序性
        self.conversation_dict = OrderedDict()
        # 当前会话列表的所有项目
        self.all_conversation_items = []
        # 最大滚动尝试次数
        self.max_scroll_attempts = 30
        # 自动点击开关
        self.auto_click_enabled = False
        # 已处理会话ID
        self.processed_ids = set()
        # 上次检查时间
        self.last_check_time = datetime.now()
        # 会话位置索引
        self.conversation_positions = {}
        
    def update_exclude_keywords(self):
        """更新排除关键词，添加自己的用户名"""
        if self.username and self.username not in WeChatConfig.EXCLUDE_KEYWORDS:
            # 添加用户名到排除列表，避免检测自己发给自己的消息
            WeChatConfig.EXCLUDE_KEYWORDS.append(self.username)
            logger.info(f"已添加用户名 '{self.username}' 到排除列表")
        
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
    
    def scroll_conversation_list(self, direction="down", max_attempts=30):
        """滚动会话列表
        
        Args:
            direction: 滚动方向，"up"向上，"down"向下
            max_attempts: 最大尝试次数
        
        Returns:
            int: 滚动后的新会话数量
        """
        if not self.conversation_list:
            logger.warning("找不到会话列表，无法滚动")
            return 0
            
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
                    return 0
            
            # 检查滚动后的会话项
            time.sleep(0.5)
            after_items = [item.Name for item in self.conversation_list.GetChildren() if hasattr(item, "Name")]
            
            # 检查是否有变化
            if before_items == after_items:
                logger.info(f"滚动{direction}后会话列表没有变化")
                return 0
            else:
                new_count = len(set(after_items) - set(before_items))
                logger.info(f"滚动{direction}后会话列表有 {new_count} 个新会话")
                return new_count
                
        except Exception as e:
            logger.error(f"滚动会话列表时出错: {e}")
            return 0
    
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
            
            # 获取点击前的聊天窗口标题，用于后续比较
            pre_chat_title = self.get_chat_window_title()
            logger.info(f"点击前的聊天窗口标题: {pre_chat_title}")
            
            # 使用控件自带的点击方法
            try:
                conversation_item.Click(simulateMove=False)
                logger.info(f"已点击会话: {name}")
                time.sleep(1.0)  # 等待界面响应
                
                # 验证是否切换到对应的聊天窗口
                return self.verify_chat_window_switched(name, pre_chat_title)
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
                    time.sleep(1.0)
                    
                    # 验证是否切换到对应的聊天窗口
                    return self.verify_chat_window_switched(name, pre_chat_title)
                except Exception as e:
                    logger.error(f"通过坐标点击会话失败: {e}")
                    return False
        except Exception as e:
            logger.error(f"点击会话过程中出错: {e}")
            return False
            
    def get_chat_window_title(self):
        """获取当前聊天窗口的标题
        
        Returns:
            str: 聊天窗口标题，如果获取失败则返回空字符串
        """
        try:
            # 尝试查找标题栏
            for control in self.wechat_window.GetChildren():
                if control.ControlTypeName == "TitleBarControl" and hasattr(control, "Name"):
                    title = control.Name
                    # 微信窗口标题通常是 "会话名称 - 微信"
                    if " - 微信" in title:
                        return title.replace(" - 微信", "")
                    return title
                    
            # 备用方法：尝试在聊天窗口顶部查找会话名称
            pane_controls = self.wechat_window.GetChildren(lambda c: c.ControlTypeName == "PaneControl")
            for pane in pane_controls:
                # 查找可能包含会话名称的文本控件
                text_controls = pane.GetChildren(lambda c: c.ControlTypeName == "TextControl")
                if text_controls and len(text_controls) > 0:
                    for text in text_controls:
                        if hasattr(text, "Name") and text.Name and len(text.Name) > 0:
                            # 通常第一个有文本的控件是会话名称
                            return text.Name
            
            return ""
        except Exception as e:
            logger.error(f"获取聊天窗口标题时出错: {e}")
            return ""
            
    def verify_chat_window_switched(self, expected_name, previous_title, max_attempts=3):
        """验证聊天窗口是否成功切换到目标会话
        
        Args:
            expected_name: 预期的会话名称
            previous_title: 切换前的窗口标题
            max_attempts: 最大尝试次数
            
        Returns:
            bool: 是否成功切换
        """
        for attempt in range(max_attempts):
            # 获取当前窗口标题
            current_title = self.get_chat_window_title()
            logger.info(f"当前聊天窗口标题: {current_title} (尝试 {attempt+1}/{max_attempts})")
            
            # 检查是否与预期名称匹配
            if expected_name in current_title or current_title == expected_name:
                logger.info(f"成功切换到目标会话: {expected_name}")
                return True
                
            # 检查是否有变化
            if current_title != previous_title and current_title:
                logger.info(f"聊天窗口已切换，但标题与预期不完全匹配 (预期: {expected_name}, 实际: {current_title})")
                # 尽管不完全匹配，但窗口已经切换，我们接受这个结果
                return True
                
            # 如果没有变化且尚未达到最大尝试次数，等待一会再次检查
            if attempt < max_attempts - 1:
                logger.info(f"聊天窗口未切换，等待重试... (预期: {expected_name}, 当前: {current_title})")
                time.sleep(1.0)
        
        logger.warning(f"无法确认聊天窗口已切换到目标会话: {expected_name}")
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
    
    def create_conversation_info(self, item, index=0):
        """从会话项创建会话信息字典
        
        Args:
            item: 会话控件项
            index: 会话索引
            
        Returns:
            dict: 会话信息字典
        """
        try:
            # 获取会话名称
            name = item.Name if hasattr(item, "Name") else f"会话 {index}"
            
            # 生成会话ID
            conv_id = self.generate_conversation_id(item)
            
            # 检查是否有未读标记
            has_unread = self.check_conversation_unread(item)
            
            # 创建会话信息
            return {
                "index": index,
                "name": name,
                "id": conv_id,
                "has_unread": has_unread,
                "item": item,  # 存储控件引用
                "last_update": datetime.now(),  # 记录最后更新时间
                "position": index,  # 记录位置
                "is_valid": True  # 标记是否有效
            }
        except Exception as e:
            logger.error(f"创建会话信息时出错: {e}")
            return None
    
    def get_initial_conversations(self):
        """获取初始会话列表
        
        Returns:
            bool: 是否成功获取
        """
        # 确保找到微信窗口
        if not self.find_wechat_window():
            logger.error("无法找到微信窗口")
            return False
            
        # 尝试找到会话列表
        if not self.find_conversation_list():
            logger.error("无法找到会话列表")
            return False
            
        # 清空之前的数据
        self.conversation_dict.clear()
        self.all_conversation_items.clear()
        self.processed_ids.clear()
        self.conversation_positions.clear()
        
        # 计数器
        valid_conversations = 0
        
        logger.info("开始获取初始会话列表...")
        
        # 尝试滚动到顶部
        logger.info("尝试滚动到列表顶部...")
        self.scroll_conversation_list(direction="up", max_attempts=10)
        
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
                    name = item.Name if hasattr(item, "Name") else f"会话 {len(self.conversation_dict)}"
                    
                    # 生成会话ID
                    conv_id = self.generate_conversation_id(item)
                    
                    # 如果已处理过该会话或应该被排除，则跳过
                    if conv_id in self.processed_ids or self.should_exclude_conversation(name):
                        continue
                        
                    # 标记为已处理
                    self.processed_ids.add(conv_id)
                    
                    # 创建会话信息
                    conversation_info = self.create_conversation_info(item, valid_conversations)
                    if not conversation_info:
                        continue
                    
                    # 添加到会话字典 - 使用ID作为键
                    self.conversation_dict[conv_id] = conversation_info
                    self.all_conversation_items.append(item)
                    
                    # 更新位置索引
                    self.conversation_positions[conv_id] = valid_conversations
                    
                    # 输出会话信息
                    unread_status = "【有未读】" if conversation_info["has_unread"] else ""
                    logger.info(f"{valid_conversations + 1}. {name} {unread_status}")
                    
                    # 获取并保存聊天记录
                    try:
                        logger.info(f"获取 '{name}' 的聊天记录")
                        
                        # 点击会话打开聊天窗口
                        if self.click_conversation(item):
                            # 等待聊天窗口加载
                            time.sleep(1.5)
                            
                            # 获取当前窗口标题，再次确认
                            current_title = self.get_chat_window_title()
                            if current_title and name not in current_title and current_title not in name:
                                logger.warning(f"聊天窗口标题 '{current_title}' 与会话名称 '{name}' 不匹配，可能切换失败")
                                # 继续尝试获取聊天记录
                            
                            # 获取聊天记录
                            chat_records = self.get_chat_records(page=3)  # 获取3页聊天记录
                            
                            # 保存聊天记录
                            if chat_records:
                                self.save_chat_records(conv_id, name, chat_records)
                                logger.info(f"已保存 '{name}' 的聊天记录，共 {len(chat_records)} 条")
                            else:
                                logger.warning(f"未获取到 '{name}' 的聊天记录")
                        else:
                            logger.warning(f"点击会话 '{name}' 失败或无法确认切换，跳过获取聊天记录")
                    except Exception as e:
                        logger.error(f"获取 '{name}' 的聊天记录时出错: {e}")
                    
                    # 计数有效会话
                    valid_conversations += 1
                    
                    # 如果已达到目标数量，结束处理
                    if valid_conversations >= WeChatConfig.MAX_CONVERSATIONS:
                        break
                        
                except Exception as e:
                    logger.error(f"处理会话项时出错: {e}")
            
            # 如果已经达到目标数量，结束循环
            if valid_conversations >= WeChatConfig.MAX_CONVERSATIONS:
                break
                
            # 尝试向下滚动以获取更多会话
            logger.info("尝试滚动以获取更多会话...")
            new_items_count = self.scroll_conversation_list(direction="down")
            
            # 如果向下滚动后没有新会话，说明已到达底部
            if new_items_count == 0:
                logger.info("已到达会话列表底部")
                break
                
            scroll_attempts += 1
            
        logger.info(f"==== 初始会话列表获取完成 (共 {valid_conversations} 个) ====")
        
        return valid_conversations > 0
    
    def get_current_visible_conversations(self):
        """获取当前可见的会话信息"""
        if not self.conversation_list:
            if not self.find_conversation_list():
                logger.error("无法获取会话列表")
                return []
        
        # 获取当前可见的会话项
        current_items = self.conversation_list.GetChildren()
        
        # 收集可见会话信息
        visible_conversations = []
        for i, item in enumerate(current_items):
            try:
                name = item.Name if hasattr(item, "Name") else f"会话 {i}"
                conv_id = self.generate_conversation_id(item)
                
                # 如果应该被排除，则跳过
                if self.should_exclude_conversation(name):
                    continue
                
                # 检查是否有未读
                has_unread = self.check_conversation_unread(item)
                
                visible_conversations.append({
                    "name": name,
                    "id": conv_id,
                    "has_unread": has_unread,
                    "item": item,
                    "index": i
                })
            except Exception as e:
                logger.error(f"获取可见会话 {i} 信息时出错: {e}")
        
        return visible_conversations
    
    def update_conversation_dict(self, visible_conversations):
        """根据当前可见会话更新会话字典
        
        Args:
            visible_conversations: 当前可见的会话列表
            
        Returns:
            bool: 是否有更新
        """
        if not visible_conversations:
            return False
            
        has_updates = False
        current_time = datetime.now()
        
        # 检查所有可见会话
        for i, conv in enumerate(visible_conversations):
            conv_id = conv["id"]
            
            # 如果是新会话
            if conv_id not in self.conversation_dict:
                # 如果已经有30个会话，找到最老的会话移除
                if len(self.conversation_dict) >= WeChatConfig.MAX_CONVERSATIONS:
                    # 找到最老的会话（最后更新时间最早的）
                    oldest_id = None
                    oldest_time = None
                    
                    for cid, cinfo in self.conversation_dict.items():
                        if oldest_time is None or cinfo["last_update"] < oldest_time:
                            oldest_time = cinfo["last_update"]
                            oldest_id = cid
                    
                    # 如果找到最老的会话，移除它
                    if oldest_id:
                        logger.info(f"移除最旧的会话: {self.conversation_dict[oldest_id]['name']}")
                        del self.conversation_dict[oldest_id]
                
                # 添加新会话
                logger.info(f"添加新会话: {conv['name']}")
                
                # 创建会话信息
                conversation_info = {
                    "index": len(self.conversation_dict),
                    "name": conv["name"],
                    "id": conv_id,
                    "has_unread": conv["has_unread"],
                    "item": conv["item"],
                    "last_update": current_time,
                    "position": i,
                    "is_valid": True
                }
                
                # 添加到会话字典
                self.conversation_dict[conv_id] = conversation_info
                has_updates = True
            else:
                # 更新现有会话
                existing_conv = self.conversation_dict[conv_id]
                
                # 检查是否有状态变化
                if existing_conv["has_unread"] != conv["has_unread"] or existing_conv["position"] != i:
                    logger.info(f"更新会话状态: {conv['name']}")
                    
                    # 更新会话信息
                    existing_conv["has_unread"] = conv["has_unread"]
                    existing_conv["position"] = i
                    existing_conv["last_update"] = current_time
                    existing_conv["item"] = conv["item"]  # 更新控件引用
                    has_updates = True
        
        # 如果有更新，重新排序会话字典
        if has_updates:
            # 基于位置排序，确保最新的会话在前面
            sorted_items = sorted(self.conversation_dict.items(), 
                                key=lambda x: x[1]["position"] if "position" in x[1] else float('inf'))
            
            # 重建有序字典
            self.conversation_dict = OrderedDict()
            for i, (conv_id, conv_info) in enumerate(sorted_items):
                conv_info["index"] = i  # 更新索引
                self.conversation_dict[conv_id] = conv_info
        
        return has_updates
    
    def check_conversation_updates(self):
        """检查会话更新状态并更新会话字典"""
        if not self.conversation_dict:
            logger.warning("没有会话可检查")
            return
            
        try:
            logger.info("开始检查会话更新状态...")
            
            # 确保会话列表存在
            if not self.conversation_list or not self.conversation_list.Exists():
                if not self.find_conversation_list():
                    logger.error("无法找到会话列表")
                    return
                    
            # 滚动到顶部以获取最新的会话
            logger.info("尝试滚动到列表顶部...")
            for _ in range(5):
                if not self.scroll_conversation_list("up"):
                    break
                time.sleep(0.2)
            
            # 获取当前可见的会话
            visible_conversations = self.get_current_visible_conversations()
            if not visible_conversations:
                logger.warning("未获取到可见会话")
                return
                
            # 更新会话字典
            has_updates = self.update_conversation_dict(visible_conversations)
            
            # 检查未读消息
            unread_count = 0
            checked_count = 0
            
            # 遍历会话字典
            for conv_id, conv in self.conversation_dict.items():
                if conv["has_unread"]:
                    unread_count += 1
                    logger.info(f"[通知] 检测到未读会话: {conv['name']}")
                    
                    # 如果启用了自动点击，点击会话查看内容
                    if self.auto_click_enabled and hasattr(conv["item"], "Click"):
                        if self.click_conversation(conv["item"]):
                            messages = self.get_conversation_messages(conv)
                            if messages:
                                logger.info(f"==== {conv['name']} 的最新消息 ====")
                                for i, msg in enumerate(messages):
                                    logger.info(f"  {i+1}. {msg}")
                
                checked_count += 1
            
            if has_updates:
                logger.info(f"会话列表已更新，现在有 {len(self.conversation_dict)} 个会话")
            
            logger.info(f"会话检查完成: 监控 {checked_count} 个会话，发现 {unread_count} 个有未读消息")
            
        except Exception as e:
            logger.error(f"检查会话更新状态时出错: {e}")
    
    def print_conversation_stats(self):
        """打印会话统计信息"""
        if not self.conversation_dict:
            logger.info("没有会话数据")
            return
            
        logger.info(f"\n==== 会话统计 (共 {len(self.conversation_dict)} 个) ====")
        logger.info(f"监控时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 计算未读会话数
        unread_count = sum(1 for conv in self.conversation_dict.values() if conv["has_unread"])
        logger.info(f"未读会话数: {unread_count}")
        
        # 打印前5个会话
        top_convs = list(self.conversation_dict.values())[:5]
        logger.info("前5个会话:")
        for i, conv in enumerate(top_convs):
            unread_mark = "【有未读】" if conv["has_unread"] else ""
            logger.info(f"  {i+1}. {conv['name']} {unread_mark}")
    
    def monitor(self, check_interval=5):
        """开始监控微信会话列表
        
        Args:
            check_interval: 检查间隔，单位为秒
        """
        logger.info("开始监控微信会话列表...")
        
        # 确保更新排除关键词
        self.update_exclude_keywords()
        
        # 首先获取初始会话列表
        if not self.get_initial_conversations():
            logger.error("初始化会话列表失败")
            return
            
        # 开始轮询
        while True:
            try:
                # 确保微信窗口存在
                if not self.wechat_window or not self.wechat_window.Exists():
                    if not self.find_wechat_window():
                        logger.warning("微信窗口不存在，等待下一次检查...")
                        time.sleep(check_interval)
                        continue
                
                # 检查会话更新
                self.check_chat_records_updates()
                
                # 等待下一次检查
                time.sleep(check_interval)
                
            except Exception as e:
                logger.error(f"监控过程中出错: {e}")
                time.sleep(check_interval)
            except KeyboardInterrupt:
                logger.info("用户中断，退出监控")
                break
                
    def fetch_all_chat_records(self):
        """获取所有会话的聊天记录"""
        count = 0
        max_count = min(len(self.conversation_dict), WeChatConfig.MAX_CONVERSATIONS)
        
        logger.info(f"准备获取 {max_count} 个会话的聊天记录")
        
        # 创建保存聊天记录的目录
        chat_records_dir = "chat_records"
        if not os.path.exists(chat_records_dir):
            os.makedirs(chat_records_dir)
            
        # 逐个获取会话的聊天记录
        for conv_id, conv in list(self.conversation_dict.items())[:max_count]:
            try:
                count += 1
                name = conv.get('name', '未知会话')
                
                # 跳过排除的会话
                if self.should_exclude_conversation(name):
                    logger.info(f"跳过排除的会话: {name}")
                    continue
                    
                logger.info(f"[{count}/{max_count}] 获取 '{name}' 的聊天记录")
                
                # 点击会话
                item = conv.get('item')
                if not item or not isinstance(item, auto.Control) or not item.Exists(1):
                    logger.warning(f"会话项 '{name}' 无效或不存在，跳过")
                    continue
                
                # 点击会话打开聊天窗口
                if not self.click_conversation(item):
                    logger.warning(f"点击会话 '{name}' 失败或无法确认切换，跳过")
                    continue
                    
                # 等待聊天窗口完全加载
                time.sleep(1.5)
                
                # 获取当前窗口标题，再次确认
                current_title = self.get_chat_window_title()
                if current_title and name not in current_title and current_title not in name:
                    logger.warning(f"聊天窗口标题 '{current_title}' 与会话名称 '{name}' 不匹配，可能切换失败")
                    # 我们仍然继续，但记录警告
                
                # 获取聊天记录
                chat_records = self.get_chat_records(page=3)  # 获取3页聊天记录，可以根据需要调整
                
                # 保存聊天记录
                if chat_records:
                    self.save_chat_records(conv_id, name, chat_records)
                    logger.info(f"已保存 '{name}' 的聊天记录，共 {len(chat_records)} 条")
                else:
                    logger.warning(f"未获取到 '{name}' 的聊天记录")
                
                # 短暂暂停，避免操作过快
                time.sleep(random.uniform(0.5, 1.0))
                
            except Exception as e:
                logger.error(f"获取 '{conv.get('name', '未知会话')}' 的聊天记录时出错: {e}")
                
        logger.info(f"聊天记录获取完成，共处理 {count} 个会话")
    
    def get_chat_records(self, page=1):
        """获取当前打开会话的聊天记录
        
        Args:
            page(int): 获取的页数，默认为1页
            
        Returns:
            list: 聊天记录列表
        """
        try:
            chat_records = []
            
            def extract_msg():
                """提取消息内容"""
                try:
                    # 获取消息列表控件
                    msg_list = self.wechat_window.ListControl(Name="消息")
                    if not msg_list.Exists(1):
                        logger.warning("未找到消息列表控件")
                        return
                        
                    # 获取所有消息项
                    all_msgs = msg_list.GetChildren()
                    if all_msgs:
                        logger.info(f"获取到 {len(all_msgs)} 条消息")
                    else:
                        logger.warning("消息列表为空")
                        return
                    
                    # 处理每条消息
                    for msg_node in all_msgs:
                        if not hasattr(msg_node, "Name") or not msg_node.Name:
                            continue
                            
                        msg = msg_node.Name
                        record = {
                            'type': 'Unknown',
                            'name': 'Unknown',
                            'msg': msg,
                            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                        
                        # 处理时间消息
                        try:
                            if hasattr(msg_node, "PaneControl") and msg_node.PaneControl().Exists() and hasattr(msg_node.PaneControl(), "Name") and msg_node.PaneControl().Name:
                                record = {
                                    'type': 'Time',
                                    'name': 'System',
                                    'msg': msg_node.PaneControl().Name,
                                    'timestamp': msg_node.PaneControl().Name  # 使用消息中的时间
                                }
                                chat_records.append(record)
                                continue
                        except:
                            pass
                            
                        # 处理系统消息
                        if msg in ['以下为新消息', '查看更多消息', '该类型文件可能存在安全风险，建议先检查文件安全性后再打开。', '已撤回']:
                            record = {
                                'type': 'System',
                                'name': 'System',
                                'msg': msg
                            }
                            chat_records.append(record)
                            continue
                            
                        # 处理撤回消息
                        if '撤回了一条消息' in msg or '尝试撤回上一条消息' in msg:
                            name_parts = msg.split(' ')
                            record = {
                                'type': 'Other',
                                'name': ''.join(name_parts[:-1]),
                                'msg': name_parts[-1]
                            }
                            chat_records.append(record)
                            continue
                            
                        # 处理红包消息
                        if msg in ['发出红包，请在手机上查看', '收到红包，请在手机上查看', '你发送了一次转账收款提醒，请在手机上查看', '你收到了一次转账收款提醒，请在手机上查看']:
                            record = {
                                'type': 'RedEnvelope',
                                'name': 'System',
                                'msg': msg
                            }
                            chat_records.append(record)
                            continue
                            
                        # 尝试获取发送者名称
                        sender_name = "Unknown"
                        try:
                            # 通常发送者名称在ButtonControl中
                            if hasattr(msg_node, "ButtonControl") and msg_node.ButtonControl(foundIndex=1).Exists():
                                sender_name = msg_node.ButtonControl(foundIndex=1).Name
                        except:
                            pass
                            
                        record = {
                            'type': 'Content',
                            'name': sender_name,
                            'msg': msg
                        }
                        chat_records.append(record)
                    
                    if chat_records:
                        logger.info(f"提取了 {len(chat_records)} 条消息")
                    
                except Exception as e:
                    logger.error(f"提取消息时出错: {e}")
            
            logger.info(f"正在获取{page}页聊天记录...")
            
            # 向上滚动查看更多消息
            for i in range(page):
                self.wechat_window.WheelUp(wheelTimes=15)
                logger.info(f"已向上滚动第{i+1}页")
                time.sleep(0.5)
                
            # 提取消息
            extract_msg()
            
            # 按时间戳排序（如果有）
            chat_records.sort(key=lambda x: x.get('timestamp', '1970-01-01 00:00:00'))
            
            return chat_records
            
        except Exception as e:
            logger.error(f"获取聊天记录时出错: {e}")
            return []
    
    def save_chat_records(self, conv_id, name, chat_records):
        """保存聊天记录到文件
        
        Args:
            conv_id: 会话ID
            name: 会话名称
            chat_records: 聊天记录列表
        """
        try:
            # 处理文件名，移除非法字符
            safe_name = re.sub(r'[\\/*?:"<>|]', "_", name)
            
            # 创建文件名
            filename = f"chat_records/{safe_name}_{conv_id}.json"
            
            # 添加时间戳
            data = {
                'conv_id': conv_id,
                'name': name,
                'last_update': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'records': chat_records
            }
            
            # 写入文件
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            logger.error(f"保存聊天记录时出错: {e}")
    
    def check_chat_records_updates(self):
        """检查所有会话的聊天记录是否有更新"""
        try:
            count = 0
            updated_count = 0
            max_count = min(len(self.conversation_dict), WeChatConfig.MAX_CONVERSATIONS)
            
            logger.info(f"开始检查 {max_count} 个会话的聊天记录更新...")
            
            # 确保微信窗口激活
            if not self.find_wechat_window():
                logger.error("无法找到微信窗口")
                return
            
            # 逐个检查会话的聊天记录
            for conv_id, conv in list(self.conversation_dict.items())[:max_count]:
                try:
                    count += 1
                    name = conv.get('name', '未知会话')
                    
                    # 跳过排除的会话
                    if self.should_exclude_conversation(name):
                        continue
                        
                    logger.info(f"[{count}/{max_count}] 检查 '{name}' 的聊天记录更新")
                    
                    # 获取会话项
                    item = conv.get('item')
                    if not item or not isinstance(item, auto.Control) or not item.Exists(1):
                        logger.warning(f"会话项不存在或无效，尝试重新查找...")
                        
                        # 尝试刷新会话列表
                        if not self.find_conversation_list():
                            logger.error("无法找到会话列表")
                            continue
                            
                        # 尝试通过名称查找会话项
                        found = False
                        visible_items = self.conversation_list.GetChildren()
                        for visible_item in visible_items:
                            if hasattr(visible_item, "Name") and visible_item.Name == name:
                                item = visible_item
                                conv['item'] = item  # 更新会话项引用
                                found = True
                                logger.info(f"重新找到会话项: {name}")
                                break
                                
                        if not found:
                            logger.warning(f"无法找到会话项: {name}")
                            continue
                    
                    # 点击会话打开聊天窗口
                    if not self.click_conversation(item):
                        logger.warning(f"点击会话 '{name}' 失败或无法确认切换，跳过")
                        continue
                        
                    # 等待聊天窗口加载
                    time.sleep(1.5)
                    
                    # 获取当前窗口标题，再次确认
                    current_title = self.get_chat_window_title()
                    if current_title and name not in current_title and current_title not in name:
                        logger.warning(f"聊天窗口标题 '{current_title}' 与会话名称 '{name}' 不匹配，可能切换失败")
                        # 继续尝试获取聊天记录，但记录警告
                    
                    # 获取最新聊天记录
                    new_records = self.get_chat_records(page=1)  # 只获取1页，减少滚动次数
                    if not new_records:
                        logger.warning(f"未获取到 '{name}' 的最新聊天记录")
                        continue
                        
                    # 读取旧记录
                    old_records = self.read_chat_records(conv_id, name)
                    
                    # 比较记录，检查是否有更新
                    has_updates = self.compare_chat_records(old_records, new_records)
                    
                    if has_updates:
                        # 合并记录
                        merged_records = self.merge_chat_records(old_records, new_records)
                        
                        # 保存更新后的记录
                        self.save_chat_records(conv_id, name, merged_records)
                        
                        logger.info(f"'{name}' 的聊天记录有更新，已保存")
                        updated_count += 1
                    else:
                        logger.info(f"'{name}' 的聊天记录没有更新")
                    
                    # 短暂暂停，避免操作过快
                    time.sleep(random.uniform(0.5, 1.0))
                    
                except Exception as e:
                    logger.error(f"检查 '{conv.get('name', '未知会话')}' 的聊天记录更新时出错: {e}")
                    
            logger.info(f"聊天记录更新检查完成，共处理 {count} 个会话，{updated_count} 个有更新")
            
        except Exception as e:
            logger.error(f"检查聊天记录更新时出错: {e}")
    
    def read_chat_records(self, conv_id, name):
        """读取保存的聊天记录
        
        Args:
            conv_id: 会话ID
            name: 会话名称
            
        Returns:
            list: 聊天记录列表
        """
        try:
            # 处理文件名，移除非法字符
            safe_name = re.sub(r'[\\/*?:"<>|]', "_", name)
            
            # 创建文件名
            filename = f"chat_records/{safe_name}_{conv_id}.json"
            
            # 如果文件不存在，返回空列表
            if not os.path.exists(filename):
                return []
                
            # 读取文件
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            return data.get('records', [])
            
        except Exception as e:
            logger.error(f"读取聊天记录时出错: {e}")
            return []
    
    def compare_chat_records(self, old_records, new_records):
        """比较新旧聊天记录，检查是否有更新
        
        Args:
            old_records: 旧聊天记录列表
            new_records: 新聊天记录列表
            
        Returns:
            bool: 是否有更新
        """
        # 如果没有旧记录，认为有更新
        if not old_records:
            return True
            
        # 如果新记录为空，认为没有更新
        if not new_records:
            return False
            
        # 创建旧记录的消息集合
        old_msg_set = set()
        for record in old_records:
            # 使用消息内容和发送者作为标识
            key = f"{record.get('name', '')}:{record.get('msg', '')}"
            old_msg_set.add(key)
            
        # 检查新记录中是否有不在旧记录中的消息
        for record in new_records:
            key = f"{record.get('name', '')}:{record.get('msg', '')}"
            if key not in old_msg_set:
                return True
                
        return False
    
    def merge_chat_records(self, old_records, new_records):
        """合并新旧聊天记录，移除重复消息
        
        Args:
            old_records: 旧聊天记录列表
            new_records: 新聊天记录列表
            
        Returns:
            list: 合并后的聊天记录列表
        """
        # 创建消息字典，以消息内容和发送者作为键
        record_dict = {}
        
        # 添加旧记录
        for record in old_records:
            key = f"{record.get('name', '')}:{record.get('msg', '')}"
            record_dict[key] = record
            
        # 添加新记录（覆盖相同的旧记录）
        for record in new_records:
            key = f"{record.get('name', '')}:{record.get('msg', '')}"
            record_dict[key] = record
            
        # 转换回列表
        merged_records = list(record_dict.values())
        
        # 按时间戳排序（如果有）
        merged_records.sort(key=lambda x: x.get('timestamp', '1970-01-01 00:00:00'))
        
        return merged_records


if __name__ == "__main__":
    try:
        logger.info("启动微信监控脚本")
        monitor = WeChatMonitor()
        # 可以在这里设置用户名
        monitor.username = "Do you want to continue ?"
        
        # 是否启用自动点击功能（默认禁用）
        monitor.auto_click_enabled = True  # 修改为True，以便可以点击会话
        
        # 尝试查找微信窗口
        if monitor.find_wechat_window():
            # 开始监控，每30秒检查一次
            monitor.monitor(check_interval=30)
        else:
            logger.error("无法找到微信窗口，请确保微信已经登录并正在运行")
    except KeyboardInterrupt:
        logger.info("用户中断，退出监控")
    except Exception as e:
        logger.error(f"程序运行出错: {e}")
