#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""
工具函数模块
包含日志记录、文件处理、清理等通用功能
"""

import time
import re
import os
from pathlib import Path
from loguru import logger


def write_failed_file(log_file, file_info):
    """
    将失败文件信息写入对应的日志文件

    Args:
        log_file: 日志文件路径
        file_info: 文件信息元组
    """
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    try:
        with open(log_file, "a", encoding="utf-8") as f:
            if log_file == "failed_files.log":
                name, url_or_type, reason = file_info
                f.write(f"[{timestamp}] {name} | {reason} | {url_or_type}\n")
            elif log_file == "no_right_files.log":
                path, name, ftype = file_info
                f.write(f"[{timestamp}] [{ftype}] {path}/{name}\n")
            elif log_file == "skipped_files.log":
                name, ftype, reason = file_info
                f.write(f"[{timestamp}] [{ftype}] {name} | {reason}\n")
    except Exception as e:
        logger.error(f"写入失败文件日志 {log_file} 时出错：{e}")


def clean_filename(filename):
    """
    清理文件名，移除不合法字符

    Args:
        filename: 原始文件名

    Returns:
        str: 清理后的文件名
    """
    filename = (filename or "").replace('\\', '_').replace(' ', '_').replace(':', '_')
    filename = filename.replace('/', '_').replace('?', '_').replace("*", "_")
    filename = filename.replace('\n', '_').strip()
    filename = re.sub(r"(?u)[^-\w.]", "", filename)
    return filename


def init_log_files():
    """
    初始化日志文件，备份旧日志并创建新日志

    Returns:
        tuple: (failed_files_log, no_right_files_log, skipped_files_log)
    """
    FAILED_FILES_LOG = "failed_files.log"
    NO_RIGHT_FILES_LOG = "no_right_files.log"
    SKIPPED_FILES_LOG = "skipped_files.log"

    log_files = [FAILED_FILES_LOG, NO_RIGHT_FILES_LOG, SKIPPED_FILES_LOG]

    for log_file in log_files:
        if os.path.exists(log_file):
            # 备份旧日志文件
            backup_name = f"{log_file}.bak"
            if os.path.exists(backup_name):
                os.remove(backup_name)
            os.rename(log_file, backup_name)

        # 创建新日志文件并写入头部
        with open(log_file, "w", encoding="utf-8") as f:
            f.write(f"# 失败文件日志 - 创建时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("# 格式: [时间戳] 文件信息\n\n")

    return FAILED_FILES_LOG, NO_RIGHT_FILES_LOG, SKIPPED_FILES_LOG


def generate_download_report(proceed_files, proceed_node, no_right_files,
                           failed_files, skipped_files, log_files):
    """
    生成详细的下载报告

    Args:
        proceed_files: 已处理的文件集合
        proceed_node: 已访问的节点集合
        no_right_files: 无权限文件列表
        failed_files: 失败文件列表
        skipped_files: 跳过文件列表
        log_files: 日志文件路径元组
    """
    FAILED_FILES_LOG, NO_RIGHT_FILES_LOG, SKIPPED_FILES_LOG = log_files

    print("\n" + "="*80)
    print("下载任务完成报告")
    print("="*80)

    # 统计信息
    total_processed = len(proceed_files)
    total_nodes = len(proceed_node)

    print(f"\n统计信息：")
    print(f"  - 总共处理的文件数：{total_processed}")
    print(f"  - 总共访问的节点数：{total_nodes}")

    # 无权限文件
    if no_right_files:
        print(f"\n❌ 无权限访问的文件 ({len(no_right_files)} 个)：")
        for i, (path, name, ftype) in enumerate(no_right_files[:20], 1):  # 只显示前20个
            print(f"  {i:2d}. [{ftype}] {path}/{name}")
        if len(no_right_files) > 20:
            print(f"     ... 还有 {len(no_right_files)-20} 个文件")

    # 下载失败的文件
    if failed_files:
        print(f"\n⚠️  下载失败的文件 ({len(failed_files)} 个)：")
        for i, (name, url_or_type, reason) in enumerate(failed_files[:20], 1):
            print(f"  {i:2d}. {name} - {reason}")
        if len(failed_files) > 20:
            print(f"     ... 还有 {len(failed_files)-20} 个文件")

    # 跳过的文件
    if skipped_files:
        print(f"\n⏭️  跳过的文件 ({len(skipped_files)} 个)：")
        for i, (name, ftype, reason) in enumerate(skipped_files[:20], 1):
            print(f"  {i:2d}. [{ftype}] {name} - {reason}")
        if len(skipped_files) > 20:
            print(f"     ... 还有 {len(skipped_files)-20} 个文件")

    # 保存详细报告到文件
    report_file = Path("download_report.txt")
    with open(report_file, "w", encoding="utf-8") as f:
        f.write("下载任务详细报告\n")
        f.write("="*80 + "\n\n")
        f.write(f"生成时间：{time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        f.write(f"统计信息：\n")
        f.write(f"  - 总共处理的文件数：{total_processed}\n")
        f.write(f"  - 总共访问的节点数：{total_nodes}\n")
        f.write(f"  - 无权限文件数：{len(no_right_files)}\n")
        f.write(f"  - 下载失败文件数：{len(failed_files)}\n")
        f.write(f"  - 跳过文件数：{len(skipped_files)}\n\n")

        f.write(f"日志文件：\n")
        f.write(f"  - 失败文件日志：{Path(FAILED_FILES_LOG).absolute()}\n")
        f.write(f"  - 无权限文件日志：{Path(NO_RIGHT_FILES_LOG).absolute()}\n")
        f.write(f"  - 跳过文件日志：{Path(SKIPPED_FILES_LOG).absolute()}\n\n")

        if no_right_files:
            f.write(f"无权限访问的文件 ({len(no_right_files)} 个)：\n")
            for path, name, ftype in no_right_files:
                f.write(f"  [{ftype}] {path}/{name}\n")
            f.write("\n")

        if failed_files:
            f.write(f"下载失败的文件 ({len(failed_files)} 个)：\n")
            for name, url_or_type, reason in failed_files:
                f.write(f"  {name} - {reason}\n")
            f.write("\n")

        if skipped_files:
            f.write(f"跳过的文件 ({len(skipped_files)} 个)：\n")
            for name, ftype, reason in skipped_files:
                f.write(f"  [{ftype}] {name} - {reason}\n")
            f.write("\n")

        f.write("失败文件统计：\n")
        f.write("-" * 40 + "\n")
        failure_types = {}
        for _, _, reason in failed_files:
            failure_types[reason] = failure_types.get(reason, 0) + 1
        for reason, count in sorted(failure_types.items(), key=lambda x: x[1], reverse=True):
            f.write(f"  {reason}: {count} 个文件\n")
        f.write("\n")

    print(f"\n📄 详细报告已保存到：{report_file.absolute()}")
    print(f"\n📝 失败文件实时日志：")
    print(f"  - 下载失败：{Path(FAILED_FILES_LOG).absolute()}")
    print(f"  - 无权限访问：{Path(NO_RIGHT_FILES_LOG).absolute()}")
    print(f"  - 跳过文件：{Path(SKIPPED_FILES_LOG).absolute()}")
    print("\n" + "="*80)