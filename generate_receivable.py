#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将发票明细表中的专票和普票合并，按单位名称汇总生成应收数据

输出:
- 应收数据sheet，包含每个客户的汇总金额和税额
"""

import pandas as pd


def generate_receivable_data(input_file: str, output_file: str):
    """
    合并专票和普票，按单位名称汇总

    Args:
        input_file: 发票明细Excel文件
        output_file: 输出Excel文件
    """
    # 读取专票和普票
    df_vat = pd.read_excel(input_file, sheet_name='专票')
    df_normal = pd.read_excel(input_file, sheet_name='普票')

    print(f"专票: {len(df_vat)} 条")
    print(f"普票: {len(df_normal)} 条")

    # 合并
    df = pd.concat([df_vat, df_normal], ignore_index=True)
    print(f"合并后: {len(df)} 条")

    # 按单位名称汇总
    receivable = df.groupby('单位名称').agg({
        '单票合计': 'sum',
        '金额': 'sum',
        '税额': 'sum',
        '发票张数': 'sum'
    }).reset_index()

    # 重命名列
    receivable.columns = ['单位名称', '单票合计', '金额', '税额', '发票张数']

    # 按金额降序排列
    receivable = receivable.sort_values('金额', ascending=False).reset_index(drop=True)

    # 添加ID
    import uuid
    receivable.insert(0, 'id', [str(uuid.uuid4()) for _ in range(len(receivable))])

    print(f"\n应收数据汇总: {len(receivable)} 个客户")
    print(f"总金额: {receivable['金额'].sum():,.2f}")
    print(f"总税额: {receivable['税额'].sum():,.2f}")

    # 保存到Excel (追加到原文件)
    with pd.ExcelWriter(output_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        receivable.to_excel(writer, sheet_name='应收数据', index=False)

    print(f"\n已保存到: {output_file} 的【应收数据】sheet")

    return receivable


if __name__ == '__main__':
    input_file = 'Docs/3月发票明细.xlsx'
    output_file = 'Docs/3月发票明细.xlsx'
    generate_receivable_data(input_file, output_file)
