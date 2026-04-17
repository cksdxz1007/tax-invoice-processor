#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将发票明细表中的专票和普票合并，按单位名称汇总生成应收数据
将进项发票汇总生成应付数据

依赖 run_full_pipeline.py 中的函数

使用方式:
    python generate_receivable.py <发票明细文件>

示例:
    python generate_receivable.py Docs/3月发票明细.xlsx
"""

import sys
import os

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 从 run_full_pipeline 导入
from run_full_pipeline import step2_generate_receivable


def main():
    if len(sys.argv) < 2:
        print("用法: python generate_receivable.py <发票明细文件>")
        print("示例: python generate_receivable.py Docs/3月发票明细.xlsx")
        sys.exit(1)

    input_file = sys.argv[1]

    if not os.path.exists(input_file):
        print(f"错误: 文件不存在: {input_file}")
        sys.exit(1)

    # 从文件名提取月份
    basename = os.path.basename(input_file)
    try:
        month = int(basename.split('月')[0])
    except:
        month = int(input("请输入月份 (1-12): "))

    output_file = input_file  # 覆盖原文件

    print("=" * 50)
    print("生成应收/应付数据")
    print("=" * 50)

    success, data = step2_generate_receivable(input_file, month, output_file)

    if success:
        receivable = data.get('receivable', [])
        payable = data.get('payable', [])
        print(f"\n应收数据: {len(receivable)} 个客户")
        print(f"应付数据: {len(payable)} 个供应商")
    else:
        print("\n生成失败")


if __name__ == '__main__':
    main()
