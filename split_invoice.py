#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
处理2026年发票统计.xlsx
动态识别分区，提取发票明细数据

支持:
- 自动识别三个分区 (专票/普票/进项)
- 根据月份参数提取前一个月的数据
- 动态识别分区起止位置
"""

import pandas as pd
import uuid


# 分区标记关键词
SECTION_MARKERS = {
    '专票': '本月开出专票',
    '普票': '本月开出普票',
    '进项': '本月收到进项',
}

# 汇总行关键词
SUMMARY_KEYWORDS = ['小计', '合计', '统计', '汇总']

# 标准列名
COLUMN_NAMES = ['单位名称', '日期', '发票张数', '发票号', '单票合计', '金额', '税额', '备注']


def get_previous_month(current_month: int) -> int:
    """
    获取前一个月份

    Args:
        current_month: 当前月份 (1-12)

    Returns:
        前一个月份 (1-12)
    """
    return 12 if current_month == 1 else current_month - 1


def find_sheet_by_month(file_path: str, month: int) -> str:
    """
    根据月份查找对应的工作表

    Args:
        file_path: Excel文件路径
        month: 月份 (1-12)

    Returns:
        工作表名称，如 "4月"
    """
    sheets = pd.ExcelFile(file_path).sheet_names
    target = f'{month}月'
    if target in sheets:
        return target
    raise ValueError(f"未找到工作表: {target}")


def find_section_boundaries(df: pd.DataFrame) -> dict:
    """
    动态识别三个分区的起止位置

    Args:
        df: 原始数据DataFrame

    Returns:
        分区边界字典 {分区名: (起始行, 结束行, 列名行)}
    """
    boundaries = {}
    first_col = df.iloc[:, 0].astype(str)

    # 查找各分区起始行
    for section, marker in SECTION_MARKERS.items():
        matches = df[first_col.str.contains(marker, na=False)].index.tolist()
        if matches:
            boundaries[section] = {'start': matches[0]}
            print(f"  找到 {section}: 起始行={matches[0]}")

    # 确定分区结束行
    sections = list(boundaries.keys())
    for i, section in enumerate(sections):
        if i < len(sections) - 1:
            next_section = sections[i + 1]
            boundaries[section]['end'] = boundaries[next_section]['start'] - 1
        else:
            # 最后一个分区，严格模式：找到本项小计行作为结束
            last_data_row = boundaries[section]['start']
            for idx in range(boundaries[section]['start'] + 1, len(df)):
                row_val = df.iloc[idx, 0]
                # 遇到汇总行或空行则停止
                if pd.isna(row_val) or any(kw in str(row_val) for kw in SUMMARY_KEYWORDS):
                    last_data_row = idx - 1
                    break
                last_data_row = idx
            boundaries[section]['end'] = last_data_row

    return boundaries


def find_column_row(df: pd.DataFrame, start: int, end: int) -> int:
    """
    在指定范围内查找列名行

    Args:
        df: 数据DataFrame
        start: 起始行
        end: 结束行

    Returns:
        列名行索引
    """
    for idx in range(start, min(end + 1, start + 5)):
        row_values = df.iloc[idx].astype(str).tolist()
        if '单位名称' in row_values and '日期' in row_values:
            return idx
    return start


def is_summary_row(row_value) -> bool:
    """
    判断是否为汇总行

    Args:
        row_value: 第一列的值

    Returns:
        True if 是汇总行
    """
    if pd.isna(row_value):
        return False
    value_str = str(row_value)
    return any(kw in value_str for kw in SUMMARY_KEYWORDS)


def extract_section_data(df: pd.DataFrame, boundaries: dict, section: str, month: int) -> pd.DataFrame:
    """
    提取指定分区的数据

    Args:
        df: 原始数据DataFrame
        boundaries: 分区边界
        section: 分区名称 (专票/普票/进项)
        month: 月份

    Returns:
        处理后的DataFrame
    """
    start = boundaries[section]['start']
    end = boundaries[section]['end']

    # 查找列名行
    col_row = find_column_row(df, start, end)
    boundaries[section]['col_row'] = col_row

    # 提取数据区域
    data_start = col_row + 1
    section_df = df.iloc[data_start:end + 1].copy()

    # 设置列名
    section_df.columns = df.iloc[col_row].tolist()
    section_df = section_df.reset_index(drop=True)

    # 过滤汇总行和空行
    section_df = section_df[
        section_df['单位名称'].notna() &
        ~section_df['单位名称'].astype(str).str.contains('|'.join(SUMMARY_KEYWORDS), na=False)
    ]

    # 添加必要字段
    section_df.insert(0, 'id', [str(uuid.uuid4()) for _ in range(len(section_df))])
    section_df['发票类型'] = section
    section_df['月份'] = month

    # 调整列顺序
    cols = ['id', '单位名称', '日期', '发票张数', '发票号', '单票合计', '金额', '税额', '备注', '发票类型', '月份']
    existing_cols = [c for c in cols if c in section_df.columns]
    section_df = section_df[existing_cols]

    print(f"  {section}: {len(section_df)} 条 (列名行={col_row}, 数据行={data_start}~{end})")

    return section_df


def split_invoice_by_month(input_file: str, output_file: str, month: int):
    """
    根据月份拆分发票数据 (提取前一个月的数据)

    Args:
        input_file: 原始Excel文件路径
        output_file: 输出Excel文件路径
        month: 当前月份，函数会自动提取前一个月的数据
    """
    # 获取前一个月份
    target_month = get_previous_month(month)
    print(f"\n{'='*50}")
    print(f"处理 {month}月 的发票数据，提取 {target_month}月 的明细")
    print(f"{'='*50}")

    # 查找对应工作表
    sheet_name = find_sheet_by_month(input_file, target_month)
    print(f"\n工作表: {sheet_name}")

    # 读取数据
    df = pd.read_excel(input_file, sheet_name=sheet_name, header=None)
    print(f"数据范围: {df.shape[0]} 行 x {df.shape[1]} 列")

    # 识别分区边界
    print("\n识别分区:")
    boundaries = find_section_boundaries(df)

    # 提取各分区数据
    print("\n提取数据:")
    results = {}
    for section in ['专票', '普票', '进项']:
        if section in boundaries:
            results[section] = extract_section_data(df, boundaries, section, target_month)

    # 保存到Excel
    print(f"\n保存到: {output_file}")
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        if '专票' in results:
            results['专票'].to_excel(writer, sheet_name='专票', index=False)
        if '普票' in results:
            results['普票'].to_excel(writer, sheet_name='普票', index=False)
        if '进项' in results:
            results['进项'].to_excel(writer, sheet_name='进项', index=False)

    # 统计
    print("\n汇总:")
    for section, data in results.items():
        amount = data['金额'].sum()
        tax = data['税额'].sum()
        print(f"  {section}: {len(data)} 条, 金额={amount:,.2f}, 税额={tax:,.2f}")


def main():
    import sys

    if len(sys.argv) < 2:
        print("用法: python split_invoice.py <当前月份>")
        print("示例: python split_invoice.py 5  # 提取4月数据")
        sys.exit(1)

    current_month = int(sys.argv[1])
    target_month = get_previous_month(current_month)

    input_file = 'Docs/2026年发票统计.xlsx'
    output_file = f'Docs/{target_month}月发票明细.xlsx'

    split_invoice_by_month(input_file, output_file, current_month)


if __name__ == '__main__':
    main()
