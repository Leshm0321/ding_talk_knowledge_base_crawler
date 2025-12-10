# !/usr/bin/env python
# -*-coding:utf-8 -*-
import time
import traceback
import os
from threading import Thread
from dotenv import load_dotenv

import requests
from DrissionPage import ChromiumPage, ChromiumOptions
from loguru import logger
from pathlib import Path
from queue import Queue

# 导入自定义工具函数
from utils import (
    write_failed_file,
    clean_filename,
    init_log_files,
    generate_download_report
)

# 加载.env配置文件
load_dotenv()

proceed_node = set()
proceed_files = set()

req_queue = Queue()
download_queue = Queue()

# 配置项 - 从.env文件读取
# 公司ID
corpId = os.getenv("CORP_ID", "")
# 组织（库）ID
target_orgid = os.getenv("TARGET_ORGID", "")
# 无权限文件
no_right_files = []  # 无权限文件
failed_files = []     # 下载失败的文件
skipped_files = []    # 跳过的文件
loggined_done = False



def process_download():
    while True:
        res = download_queue.get(block=True)
        if not res:
            continue
        try:
            node_info, url, headers, cookies, save_path, save_name = res
            p = Path(save_path)
            # 创建文件夹
            os.makedirs(p.absolute(), exist_ok=True)
            # 安全地处理cookies
            if cookies:
                if hasattr(cookies, '__iter__'):
                    cookies = {x["name"]: x["value"] for x in cookies}
                else:
                    cookies = {}
            else:
                cookies = {}

            download_success = False
            for retry_times in range(10):
                try:
                    # 安全地处理headers
                    if headers:
                        # 过滤掉HTTP/2伪头部字段
                        filtered_headers = {k: v for k, v in headers.items()
                                         if not k.startswith(':') and k.lower() not in ['host', 'connection']}
                    else:
                        filtered_headers = {}
                    req = requests.request(method="get", url=url, headers=filtered_headers, cookies=cookies)
                    filename = str(url).split("?")[0].split("/")[-1]
                    save_path = p.joinpath(filename)
                    if req.status_code == 200:
                        with open(save_path.absolute(), "wb") as f:
                            f.write(req.content)
                        download_success = True
                        break
                    else:
                        raise Exception(f"下载失败，返回状态码{req.status_code},内容：{req.content}")
                except Exception as e:
                    logger.error(f"下载文件{url} 重试{retry_times} 出错：{e}")
                    time.sleep(5)
                    continue
            if not download_success:
                logger.error(f"下载文件{url}失败，推回节点到浏览器进行重试")
                # 记录失败文件信息
                if 'name' in node_info:
                    file_info = (node_info.get('name', ''), url, "下载失败")
                    if file_info not in failed_files:
                        failed_files.append(file_info)
                        write_failed_file("failed_files.log", file_info)
                q.put(node_info)
        except Exception as e:
            logger.error(f"下载{res}出错 {e}：{traceback.format_exc()}")

def request_repeater(q):
    while True:
        res = req_queue.get(block=True)
        data = None
        if res:
            if "/dentry/list?" not in str(res.url):
                logger.info(f"跳过{str(res.url)}")
                continue
            for _ in range(10):
                try:
                    logger.info(f"二次请求{res.url}，待请求长度：{req_queue.qsize()}")
                    # 安全地获取cookies和headers
                    cookies = {}
                    if hasattr(res.request, 'cookies') and res.request.cookies:
                        if hasattr(res.request.cookies, '__iter__'):
                            cookies = {x["name"]: x["value"] for x in res.request.cookies}

                    headers = {}
                    if hasattr(res.request, 'headers') and res.request.headers:
                        headers = res.request.headers

                    # 过滤掉HTTP/2伪头部字段
                    filtered_headers = {k: v for k, v in headers.items()
                                     if not k.startswith(':') and k.lower() not in ['host', 'connection']}

                    data = requests.request(method="get", url=res.url, headers=filtered_headers,
                                            cookies=cookies)
                    data = data.json()["data"]
                    logger.info(f"二次请求完成，待请求长度：{req_queue.qsize()}")
                    break
                except Exception as e:
                    logger.error(f"二次请求{res.url} 出错：{e}")
                    time.sleep(5)
                    continue
            if data:
                process_req(q, data)
            else:
                logger.error(f"二次请求{res.url} 失败次数超过10，放弃")


def process_req(q, data):
    if not data:
        return
    if "children" in data:
        process_node_name = data['name']
        item_list = data["children"]
        added_names = []
        for node_info in item_list:
            node_name = node_info['name']
            node_uuid = node_info['dentryUuid']
            if node_uuid not in proceed_node:
                added_names.append(node_name)
                q.put(node_info)
                proceed_node.add(node_uuid)
        if added_names:
            logger.info(f"队列长度：{q.qsize()} 从【{process_node_name}】 添加子节点{len(added_names)}个：{', '.join(added_names)}")

class Processer:

    def __init__(self, q, index=0):
        self.idx = index
        self.q = q
        self.page = ChromiumPage(ChromiumOptions().set_local_port(int(f"933{index}")).set_user_data_path(f'data{index}'))
        package_urls = ['box/api/v2/dentry/list?']
        self.page.listen.start(package_urls, res_type=True)
        self.page.get(f'https://alidocs.dingtalk.com/i/desktop/spaces/?corpId={corpId}')
        self.inited = False
        self.headers = {}
        self.cookies = {}

    def run(self):
        empty_count = 0
        while True:
            if loggined_done and not self.inited:
                self.inited = True
                # 打开组织页面
                self.page.get(f'https://alidocs.dingtalk.com/i/spaces/{target_orgid}/overview?corpId={corpId}')
            self.block_wait()
            while self.page.listen._caught.qsize():
                res = self.page.listen.wait(timeout=5)
                if res:
                    if res.response and res.response.body and res.response.body.get("data"):
                        data = res.response.body["data"]
                        process_req(self.q, data)
                        try:
                            # 更新最新header以及cookies
                            if hasattr(res, 'request') and res.request:
                                self.headers = getattr(res.request, 'headers', {})
                                self.cookies = getattr(res.request, 'cookies', {})
                        except Exception:
                            pass

                    else:
                        # 确保res对象有效才放入队列
                        if res and hasattr(res, 'url') and res.url:
                            req_queue.put(res)

            if not self.q.empty():
                item = self.q.get()
                for retry in range(4):
                    try:
                        self.process_node(item)
                        break
                    except Exception as e:
                        logger.error(f"处理{item}时发生错误：{e} 重试{retry+1}")
                empty_count = 0
                continue

            if empty_count > 30:
                logger.info(f"[{self.idx}] 退出")
                break
            empty_count += 1
            time.sleep(5)

        self.page.close()
        self.page.browser.quit()

    def block_wait(self):
        time.sleep(1)
        while not self.page.listen.wait_silent(targets_only=True):
            time.sleep(1)

    def process_node(self, node_info, load_page=True):
        node_name = node_info['name']
        node_uuid = node_info['dentryUuid']
        ancestorList = node_info['ancestorList']
        # if node_uuid in proceed_node:
        #     logger.info(f"[{self.idx}]jump{node_uuid}")
        #     return

        parent_node_name = "根节点"
        if ancestorList:
            parent_node_name = ancestorList[-1]['name']
        logger.info(f"[{self.idx}] 开始处理节点:{node_name} 父节点：{parent_node_name}")
        # 直接跳转页面
        if load_page:
            self.page.get(f"https://alidocs.dingtalk.com/i/nodes/{node_uuid}")
        self.block_wait()
        # 判断是否页面白屏
        if node_info.get('contentType') == 'alidoc' or node_info.get('dentryType') == 'file':
            logger.info(f"[{self.idx}] {node_name}是文件，继续处理")
            success = self.process_file(node_info)
            if not success:
                logger.info(f"[{self.idx}] {node_name} 文件 处理失败，推回队列 后续重试")
                self.q.put(node_info)
            # 选中节点
            find_div = f"@data-rbd-draggable-id={node_uuid}"
            try:
                item = self.scroll_to_see(find_div)
                self.to_item(item)
            except Exception as e:
                logger.info(f"[{self.idx}] {find_div}: {e} {traceback.format_exc()}")
                self.process_node(node_info, load_page=False)

        else:
            # 选中节点
            find_div = f"@data-rbd-draggable-id={node_uuid}"
            try:
                button = self.scroll_to_see(find_div)
                if not button:
                    self.process_node(node_info)
                time.sleep(0.5)
                self.to_item(button)
                button.click()
                time.sleep(0.5)
            except Exception as e:
                logger.info(f"[{self.idx}] {find_div}: {e} {traceback.format_exc()}")
                self.process_node(node_info, load_page=False)
    def to_item(self, item):
        # Get element position using DrissionPage's methods
        # ElementRect has location property which is a tuple (x, y)
        location = item.rect.location
        self.page.scroll.to_location(location[0], location[1])

    def check_alert(self):
        for i in range(2):
            # 检查是否有按钮 继续导出
            has_limit = self.page.eles("tag:button@@text():继续导出", timeout=0.5)
            if has_limit:
                has_limit[0].click()
                break

    def scroll_to_see(self, loc,retry_times=0):
        if retry_times > 5:
            return
        try:
            # 先尝试直接找
            client_now = self.page.run_js(
                'return document.getElementsByClassName("MAINSITE_CATALOG-node-tree-list")[0].scrollTop')
            client_height = self.page.run_js(
                'return document.getElementsByClassName("MAINSITE_CATALOG-node-tree-list")[0].clientHeight')
            tree = self.page.ele(".:MAINSITE_CATALOG-node-tree-list")
            item = self.scroll(tree, client_now, client_height, loc)
            if item:
                return item
            else:
                return self.scroll_to_see(loc,retry_times+1)
        except Exception as e:
            logger.error(f"滚动时出错：{e}，等待重试")
            return self.scroll_to_see(loc,retry_times+1)

    def scroll(self, tree, start, client_height, loc):
        if start == 0:
            tree.scroll.to_top()
            tree.scroll.to_location(0, 0)
            time.sleep(0.5)
        item = self.page.ele(loc, timeout=2)
        if item:
            return item
        last_scrollTop = None
        start_height = int(start)
        to_height = self.page.run_js(
            'return document.getElementsByClassName("MAINSITE_CATALOG-node-tree-list")[0].scrollHeight')
        # 每次滚动的高度
        roll_height = 300
        while start_height < to_height:
            to_height = self.page.run_js(
                'return document.getElementsByClassName("MAINSITE_CATALOG-node-tree-list")[0].scrollHeight')
            if last_scrollTop is not None and last_scrollTop == to_height and (
                    start_height > (to_height / 2)):
                return self.scroll(tree, 0, client_height, loc)
            last_scrollTop = self.page.run_js(
                'return document.getElementsByClassName("MAINSITE_CATALOG-node-tree-list")[0].scrollTop')
            logger.info(f"[{self.idx}] 正在滚动【高度信息：当前：{last_scrollTop or 0}->目标：{start_height}/整体{to_height}】")
            start_height += roll_height
            tree.scroll.to_location(0, start_height)
            item = self.page.ele(loc, timeout=3)
            if item:
                return item

    def process_file(self, node_info, retry_times=0):
        node_name = node_info['name']
        file_type = node_name.split(".")[-1]
        node_uuid = node_info['dentryUuid']
        ancestor_path = [clean_filename(x['name']) for x in node_info['ancestorList']]
        if node_uuid in proceed_files:
            return True
        proceed_files.add(node_uuid)
        self.block_wait()
        file_path = '\\'.join([clean_filename(x) for x in ancestor_path])
        node_name = clean_filename(node_name.rsplit(".", 1)[0])
        logger.info(f"[{self.idx}] 处理文件:{node_name} 路径：{file_path} 文件类型：{file_type}")
        path = Path(".").absolute()
        path = path.joinpath(target_orgid)
        path = path.joinpath(file_path)
        os.makedirs(str(path.absolute()), exist_ok=True)
        fname = path.joinpath(node_name)
        if fname.exists() and fname.is_dir() and len(list(os.listdir(fname))) > 0:
            logger.info(f"[{self.idx}] 节点已完成下载：{fname} 跳过。")
            return True
        if retry_times > 2:
            self.page.refresh()
        if retry_times > 5:
            logger.error(f"请求节点：{node_name}，{ancestor_path}出错次数超过10次，放弃")
            no_right_info = (file_path, node_name, file_type)
            failed_info = (node_name, file_type, f"重试次数超过限制（{retry_times}次）")
            no_right_files.append(no_right_info)
            failed_files.append(failed_info)
            write_failed_file("no_right_files.log", no_right_info)
            write_failed_file("failed_files.log", failed_info)
            proceed_files.remove(node_uuid)
            return
        # 选中节点
        find_div = f"@data-rbd-draggable-id={node_uuid}"
        try:
            item = self.scroll_to_see(find_div)
            self.to_item(item)
            item.click()
        except Exception as e:
            logger.info(f"[{self.idx}] {find_div}: {e}")
            return self.process_file(node_info, retry_times+1)
        # 判断是否无权限访问
        notice_eles = self.page.eles("@data-item-key=apply-title-view") or []
        for ne in notice_eles:
            if "暂无权限访问" in str(ne.text):
                no_right_info = (file_path, node_name, file_type)
                no_right_files.append(no_right_info)
                write_failed_file("no_right_files.log", no_right_info)
                logger.info(f"[{self.idx}] 节点：{node_name} 无访问权限，跳过")
                return True

        # 如果是链接
        if file_type == "dlink":
            file_type = node_info['linkSourceInfo']['extension']
            logger.info(f"[{self.idx}] 链接文件：{fname} 真实文件类型为：{file_type}")
        self.page.set.download_path(str(fname.absolute()))
        self.page.set.download_file_name(node_name)
        self.page.set.when_download_file_exists("skip")
        time.sleep(5)
        try:
            download_task = False
            last_err = None
            if "adoc" in file_type:
                limited_toolbar = self.page.eles("@data-testid=doc-header-more-button", timeout=2)
                if limited_toolbar:
                    for i in range(5):
                        try:
                            limited_toolbar[0].click()
                            time.sleep(0.5)
                            self.page.ele("@data-item-key=export").click()
                            self.page.ele("@data-item-key=exportAsWord").click()
                            self.check_alert()
                            download_task = self.page.wait.download_begin(timeout=120)
                            last_err = None
                            break
                        except Exception as err: 
                            last_err = err
                            time.sleep(3)
                            continue
                else:
                    for i in range(5):
                        try:
                            normal_toolbar = self.page.eles("@data-testid=bi-toolbar-menu", timeout=2)
                            if normal_toolbar:
                                normal_toolbar[0].click()
                                self.page.ele("@data-testid=bi-toolbar-menu").click()
                                time.sleep(0.5)
                                self.page.ele("@data-testid=menu-item-J_file").click()
                                time.sleep(0.5)
                                self.page.ele("@data-testid=menu-item-J_fileExport").click()
                                time.sleep(0.5)
                                self.page.ele("@data-testid=menu-item-J_exportAsWord").ele("text:Word").click()
                                self.check_alert()
                                download_task = self.page.wait.download_begin(timeout=120)
                                last_err = None
                                break
                        except Exception as err: 
                            last_err = err
                            time.sleep(3)
                            continue
                    else:
                        no_right_info = (file_path, node_name, file_type)
                no_right_files.append(no_right_info)
                write_failed_file("no_right_files.log", no_right_info)
            elif "axls" in file_type:
                limited_toolbar = self.page.eles("@data-testid=doc-header-more-button", timeout=2)
                if limited_toolbar:
                    for i in range(5):
                        try:
                            limited_toolbar[0].click()
                            time.sleep(0.5)
                            self.page.ele("@data-item-key=DOWNLOAD_AS").click()
                            self.page.ele("@data-item-key=EXCEL").click()
                            self.check_alert()
                            download_task = self.page.wait.download_begin(timeout=120)
                            last_err = None
                            break
                        except Exception as err: 
                            last_err = err
                            time.sleep(3)
                            continue
                else:
                    for i in range(5):
                        try:
                            normal_toolbar = self.page.eles("#wiki-new-sheet-iframe")
                            if normal_toolbar:
                                normal_toolbar[0].ele(
                                    "@data-testid=submenu-menubar-table").ele("text:表格").click()
                                time.sleep(0.5)
                                self.page.ele("#wiki-new-sheet-iframe").ele(
                                    "@data-testid=submenu-export-excel").ele("text:下载为").click()
                                time.sleep(0.5)
                                self.page.ele("#wiki-new-sheet-iframe").ele("text:Excel").click()
                                self.check_alert()
                                download_task = self.page.wait.download_begin(timeout=120)
                                last_err = None
                                break
                        except Exception as err: 
                            last_err = err
                            time.sleep(3)
                            continue
                    else:
                        for i in range(5):
                            try:
                                download_button = self.page.eles("@data-item-key=download", timeout=1)
                                if download_button:
                                    download_button[0].click()
                                    self.check_alert()
                                    download_task = self.page.wait.download_begin(timeout=120)
                                    last_err = None
                                    break
                                else:
                                    no_right_info = (file_path, node_name, file_type)
                                    no_right_files.append(no_right_info)
                                    write_failed_file("no_right_files.log", no_right_info)
                            except Exception as err:
                                last_err = err
                                time.sleep(3)
                                continue

            elif "pptx" in file_type or "ppt" in file_type:
                # 处理PPT文件
                logger.info(f"[{self.idx}] 处理PPT文件：{fname}")
                limited_toolbar = self.page.eles("@data-testid=doc-header-more-button", timeout=2)
                if limited_toolbar:
                    # 有限制工具栏的处理方式
                    for i in range(5):
                        try:
                            limited_toolbar[0].click()
                            time.sleep(0.5)
                            # 尝试查找导出选项
                            export_menus = self.page.eles("@data-item-key=export")
                            if export_menus:
                                export_menus[0].click()
                                time.sleep(0.5)
                                # 尝试导出为PowerPoint
                                ppt_export = self.page.eles("@data-item-key=exportAsPPT")
                                if ppt_export:
                                    ppt_export[0].click()
                                    self.check_alert()
                                    download_task = self.page.wait.download_begin(timeout=120)
                                    last_err = None
                                    break
                                else:
                                    # 尝试导出为PDF
                                    pdf_export = self.page.eles("@data-item-key=exportAsPDF")
                                    if pdf_export:
                                        pdf_export[0].click()
                                        self.check_alert()
                                        download_task = self.page.wait.download_begin(timeout=120)
                                        last_err = None
                                        break
                            last_err = Exception("未找到PPT导出选项")
                            time.sleep(3)
                            continue
                        except Exception as err:
                            last_err = err
                            time.sleep(3)
                            continue
                else:
                    # 正常工具栏的处理方式
                    for i in range(5):
                        try:
                            normal_toolbar = self.page.eles("@data-testid=bi-toolbar-menu", timeout=2)
                            if normal_toolbar:
                                normal_toolbar[0].click()
                                time.sleep(0.5)
                                # 文件菜单
                                self.page.ele("@data-testid=menu-item-J_file").click()
                                time.sleep(0.5)
                                # 导出子菜单
                                self.page.ele("@data-testid=menu-item-J_fileExport").click()
                                time.sleep(0.5)

                                # 尝试找到PowerPoint导出选项
                                ppt_menu = self.page.ele("@data-testid=menu-item-J_exportAsPPT")
                                if ppt_menu:
                                    ppt_menu.ele("text:PowerPoint").click()
                                    self.check_alert()
                                    download_task = self.page.wait.download_begin(timeout=120)
                                    last_err = None
                                    break
                                else:
                                    # 如果没有PPT导出，尝试直接下载
                                    download_button = self.page.eles("@data-item-key=download", timeout=2)
                                    if download_button:
                                        download_button[0].click()
                                        self.check_alert()
                                        download_task = self.page.wait.download_begin(timeout=120)
                                        last_err = None
                                        break
                                    else:
                                        # 尝试PDF导出
                                        pdf_menu = self.page.ele("@data-testid=menu-item-J_exportAsPDF")
                                        if pdf_menu:
                                            pdf_menu.ele("text:PDF").click()
                                            self.check_alert()
                                            download_task = self.page.wait.download_begin(timeout=120)
                                            last_err = None
                                            break
                        except Exception as err:
                            last_err = err
                            time.sleep(3)
                            continue
                    else:
                        # 所有尝试都失败，记录为无权限
                        no_right_info = (file_path, node_name, file_type)
                        no_right_files.append(no_right_info)
                        write_failed_file("no_right_files.log", no_right_info)
                        logger.warning(f"[{self.idx}] PPT文件 {node_name} 无法导出或下载")

            elif "docx" in file_type or "doc" in file_type:
                # 处理Word文档
                logger.info(f"[{self.idx}] 处理Word文档：{fname}")
                # Word文档通常可以通过标准下载按钮获取
                # 也可以尝试导出功能
                try:
                    download_button = self.page.eles("@data-item-key=download", timeout=5)
                    if download_button:
                        download_button[0].click()
                        self.check_alert()
                        download_task = self.page.wait.download_begin(timeout=120)
                    else:
                        # 尝试通过工具栏导出
                        limited_toolbar = self.page.eles("@data-testid=doc-header-more-button", timeout=2)
                        if limited_toolbar:
                            limited_toolbar[0].click()
                            time.sleep(0.5)
                            self.page.ele("@data-item-key=export").click()
                            self.page.ele("@data-item-key=exportAsWord").click()
                            self.check_alert()
                            download_task = self.page.wait.download_begin(timeout=120)
                        else:
                            download_task = False
                except Exception as err:
                    logger.error(f"[{self.idx}] 处理Word文档失败: {err}")
                    download_task = False

            elif "xlsx" in file_type or "xls" in file_type or "csv" in file_type:
                # 处理Excel文件
                logger.info(f"[{self.idx}] 处理Excel文件：{fname}")
                # Excel文件通常可以直接下载
                try:
                    download_button = self.page.eles("@data-item-key=download", timeout=5)
                    if download_button:
                        download_button[0].click()
                        self.check_alert()
                        download_task = self.page.wait.download_begin(timeout=120)
                    else:
                        download_task = False
                except Exception as err:
                    logger.error(f"[{self.idx}] 处理Excel文件失败: {err}")
                    download_task = False

            elif "pdf" in file_type:
                # 处理PDF文件
                logger.info(f"[{self.idx}] 处理PDF文件：{fname}")
                try:
                    download_button = self.page.eles("@data-item-key=download", timeout=5)
                    if download_button:
                        download_button[0].click()
                        self.check_alert()
                        download_task = self.page.wait.download_begin(timeout=120)
                    else:
                        download_task = False
                except Exception as err:
                    logger.error(f"[{self.idx}] 处理PDF文件失败: {err}")
                    download_task = False

            elif "txt" in file_type or "md" in file_type or "log" in file_type:
                # 处理文本文件
                logger.info(f"[{self.idx}] 处理文本文件：{fname}")
                try:
                    download_button = self.page.eles("@data-item-key=download", timeout=5)
                    if download_button:
                        download_button[0].click()
                        self.check_alert()
                        download_task = self.page.wait.download_begin(timeout=120)
                    else:
                        # 文本文件可能需要先打开查看
                        # 尝试获取文本内容并保存
                        content_element = self.page.ele("pre", timeout=5) or self.page.ele(".content", timeout=5)
                        if content_element:
                            # 保存文本内容
                            text_content = content_element.text
                            text_file = Path(fname.absolute()).with_suffix('.txt')
                            with open(text_file, 'w', encoding='utf-8') as f:
                                f.write(text_content)
                            logger.info(f"[{self.idx}] 文本内容已保存到: {text_file}")
                            download_task = type('obj', (object,), {'is_done': True, 'state': 'completed'})()
                        else:
                            download_task = False
                except Exception as err:
                    logger.error(f"[{self.idx}] 处理文本文件失败: {err}")
                    download_task = False

            elif file_type.lower() in ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'svg', 'webp']:
                # 处理图片文件
                logger.info(f"[{self.idx}] 处理图片文件：{fname}")
                try:
                    # 尝试右键保存图片
                    img_element = self.page.ele("img", timeout=5)
                    if img_element:
                        img_element.right_click()
                        time.sleep(1)
                        # 查找保存图片选项
                        save_option = self.page.ele("text:图片另存为", timeout=2) or \
                                     self.page.ele("text:Save image as", timeout=2)
                        if save_option:
                            save_option.click()
                            download_task = self.page.wait.download_begin(timeout=120)
                        else:
                            download_task = False
                    else:
                        download_button = self.page.eles("@data-item-key=download", timeout=5)
                        if download_button:
                            download_button[0].click()
                            self.check_alert()
                            download_task = self.page.wait.download_begin(timeout=120)
                        else:
                            download_task = False
                except Exception as err:
                    logger.error(f"[{self.idx}] 处理图片文件失败: {err}")
                    download_task = False

            elif file_type.lower() in ['zip', 'rar', '7z', 'tar', 'gz']:
                # 处理压缩文件
                logger.info(f"[{self.idx}] 处理压缩文件：{fname}")
                try:
                    download_button = self.page.eles("@data-item-key=download", timeout=5)
                    if download_button:
                        download_button[0].click()
                        self.check_alert()
                        download_task = self.page.wait.download_begin(timeout=120)
                    else:
                        download_task = False
                except Exception as err:
                    logger.error(f"[{self.idx}] 处理压缩文件失败: {err}")
                    download_task = False

            else:
                # 处理其他类型/未知格式文件的下载
                logger.info(f"[{self.idx}] 处理未知格式文件：{fname} (类型: {file_type})")
                download_task = False
                need_restart = False
                last_err = None

                # 尝试多种方式下载文件
                for attempt in range(3):
                    try:
                        if attempt == 0:
                            # 第一次尝试：查找标准下载按钮
                            download_button = self.page.eles("@data-item-key=download", timeout=5)
                            if download_button:
                                logger.info(f"[{self.idx}] 找到标准下载按钮，尝试下载")
                                download_button[0].click()
                                self.check_alert()
                                download_task = self.page.wait.download_begin(timeout=120)
                                break
                            else:
                                logger.info(f"[{self.idx}] 未找到标准下载按钮")

                        elif attempt == 1:
                            # 第二次尝试：查找其他可能的下载按钮
                            # 尝试不同的下载按钮选择器
                            download_selectors = [
                                "text:下载",
                                "text:Download",
                                "@aria-label*=下载",
                                "@aria-label*=Download",
                                "button:download",
                                ".download",
                                "[class*=download]"
                            ]

                            for selector in download_selectors:
                                download_elements = self.page.eles(selector, timeout=1)
                                if download_elements:
                                    logger.info(f"[{self.idx}] 找到下载元素: {selector}")
                                    download_elements[0].click()
                                    self.check_alert()
                                    download_task = self.page.wait.download_begin(timeout=120)
                                    break
                            if download_task:
                                break

                        elif attempt == 2:
                            # 第三次尝试：查找文件菜单并导出
                            # 尝试通过文件菜单导出
                            toolbar_menus = self.page.eles("@data-testid=bi-toolbar-menu", timeout=2)
                            if toolbar_menus:
                                logger.info(f"[{self.idx}] 尝试通过文件菜单导出")
                                toolbar_menus[0].click()
                                time.sleep(0.5)

                                # 文件菜单
                                file_menu = self.page.ele("@data-testid=menu-item-J_file", timeout=2)
                                if file_menu:
                                    file_menu.click()
                                    time.sleep(0.5)

                                    # 导出或下载
                                    export_menu = self.page.ele("@data-testid=menu-item-J_fileExport", timeout=2)
                                    if export_menu:
                                        export_menu.click()
                                        time.sleep(0.5)

                                        # 查找原格式下载
                                        download_original = self.page.ele("text:原格式", timeout=2) or \
                                                         self.page.ele("text:Original", timeout=2) or \
                                                         self.page.ele("text:下载", timeout=2)
                                        if download_original:
                                            download_original.click()
                                            self.check_alert()
                                            download_task = self.page.wait.download_begin(timeout=120)
                                            break

                    except Exception as err:
                        logger.warning(f"[{self.idx}] 下载尝试 {attempt+1} 失败: {err}")
                        last_err = err
                        time.sleep(2)
                        continue

                # 处理下载结果
                if download_task:
                    logger.info(f"[{self.idx}] 成功触发下载任务: {fname}")
                    # 等待下载
                    while not download_task.is_done:
                        time.sleep(.5)
                    if not download_task.final_path and not download_task.state == "skipped":
                        logger.info(f"[{self.idx}] 下载{fname} 任务失败 任务最终状态：{download_task.state}")
                        if "blob" not in download_task.url:
                            # 确保headers和cookies是有效的
                            headers = getattr(self, 'headers', {})
                            cookies = getattr(self, 'cookies', {})
                            res = (node_info, download_task.url, headers, cookies, str(fname.absolute()), node_name)
                            download_queue.put(res)
                            logger.info(f"[{self.idx}] 生成下载{fname}任务")
                else:
                    need_restart = True
                    logger.error(f"[{self.idx}] 所有下载尝试都失败: {fname}")

                    # 如果是未知格式，记录为无法处理而不是无权限
                    skipped_info = (node_name, file_type, f"未知格式，下载失败")
                    skipped_files.append(skipped_info)
                    write_failed_file("skipped_files.log", skipped_info)
            if last_err:
                raise last_err
            need_restart = False
            if not download_task:
                need_restart = True
            else:
                # 等待下载
                while not download_task.is_done:
                    time.sleep(.5)
                if not download_task.final_path and not download_task.state == "skipped":
                    logger.info(f"[{self.idx}] 下载{fname} 任务失败 任务最终状态：{download_task.state}")
                    if "blob" not in download_task.url:
                        res = (node_info, download_task.url, self.headers, self.cookies, str(fname.absolute()), node_name)
                        download_queue.put(res)
                        logger.info(f"[{self.idx}] 生成下载{fname}任务")
            if need_restart:
                logger.error(f"[{self.idx}] 下载：{fname} 未完成任务生成就结束了，重试一次")
                proceed_files.remove(node_uuid)
                return self.process_file(node_info, retry_times + 1)
        except Exception as e:
            logger.error(f"[{self.idx}] 下载：{fname} 时出现问题，可能是无下载权限造成的：{e} {traceback.format_exc()}")
            # no_right_files.append((file_path, node_name, file_type))
            proceed_files.remove(node_uuid)
            return self.process_file(node_info, retry_times+1)
        logger.info(f"[{self.idx}] 已完成处理文件:{node_name} 路径：{file_path} 文件类型：{file_type}，等待..")
        time.sleep(0.5)
        return True


if __name__ == "__main__":
    # 初始化日志文件
    FAILED_FILES_LOG, NO_RIGHT_FILES_LOG, SKIPPED_FILES_LOG = init_log_files()

    threads = []
    q = Queue()
    logger.info("启动浏览器。。。")
    for i in range(5):
        thread = Thread(target=request_repeater, args=(q,))
        thread.start()

    for i in range(5):
        thread = Thread(target=process_download, args=())
        thread.start()

    for i in range(5):
        thread = Thread(target=Processer(q, i).run, args=())
        thread.start()
    input(f"请完成所有浏览器的登录，并在完成后任意键继续")
    loggined_done = True
    input(f"全部下载完成后任意键继续")


    [x.join() for x in threads]
    time.sleep(5)
    while q.qsize():
        time.sleep(10)

    # 生成详细的下载报告
    log_files = (FAILED_FILES_LOG, NO_RIGHT_FILES_LOG, SKIPPED_FILES_LOG)
    generate_download_report(proceed_files, proceed_node, no_right_files,
                           failed_files, skipped_files, log_files)

    input("\n全部抓取完成，任意键退出")

