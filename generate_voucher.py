#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
根据发票明细表生成凭证数据

严格按照凭证模板.xlsx的73个字段生成，不增加额外字段。

数据来源:
- 发票明细: Docs/3月发票明细.xlsx (应收数据 sheet)
- 客户档案: SQLite数据库 data.db (customers表)
- 摘要格式: X月应收账款
"""

import pandas as pd
import sqlite3
from openpyxl import Workbook


# 凭证模板字段顺序 (73个字段，严格按照模板)
VOUCHER_FIELDS = [
    '会计期间', '凭证类别字', '凭证类别排序号', '凭证编号', '行号', '制单日期',
    '附单据数', '制单人', '审核人', '记账人', '记账标志', '出纳人', '凭证标志',
    '凭证头自定义项1', '凭证头自定义项2', '摘要', '科目编码', '币种', '借方金额',
    '贷方金额', '外币借方金额', '外币贷方金额', '汇率', '数量借方', '数量贷方',
    '结算方式编码', '票号', '票号发生日期', '部门编码', '职员编码', '客户编码',
    '供应商编码', '项目编码', '项目大类编码', '业务员', '对方科目编码', '银行帐两清标志',
    '往来帐两清标志', '是否核销', '外部凭证帐套号', '外部凭证会计年度', '外部凭证系统名称',
    '外部凭证系统版本号', '外部凭证制单日期', '外部凭证会计期间', '外部凭证业务类型',
    '外部凭证业务号', '日期', '标志', '外部凭证单据号', '凭证是否可修改', '凭证分录是否可增删',
    '凭证合计金额是否保值', '分录数值是否可修改', '分录科目是否可修改', '分录受控科目可用状态',
    '分录往来项是否可修改', '分录部门是否可修改', '分录项目是否可修改', '分录往来项是否必输',
    '自定义字段1', '自定义字段2', '自定义字段3', '自定义字段4', '自定义字段5',
    '自定义字段6', '自定义字段7', '自定义字段8', '自定义字段9', '自定义字段10',
    '现金项目编号', '现金借方', '现金贷方'
]


def get_customer_code(db_path: str, customer_name: str) -> tuple:
    """根据客户名称获取客户编码，返回(编码, 是否匹配)"""
    conn = sqlite3.connect(db_path)
    cursor = conn.execute(
        'SELECT 客户编号 FROM customers WHERE 客户名称 = ? OR 客户简称 = ? OR 总公司全称 = ?',
        (customer_name, customer_name, customer_name)
    )
    result = cursor.fetchone()
    conn.close()
    if result:
        return str(result[0]), True
    return '', False


def create_entry(month: int, year: int, voucher_no: int, row_no: int,
                 summary: str, subject_code: str, debit: float, credit: float,
                 customer_code: str, customer_name: str, counter_subject: str,
                 voucher_date: str) -> dict:
    """创建单条分录，73个字段全部填充"""
    entry = {field: '' for field in VOUCHER_FIELDS}

    entry['会计期间'] = month
    entry['凭证类别字'] = '记'
    entry['凭证类别排序号'] = 1
    entry['凭证编号'] = voucher_no
    entry['行号'] = row_no
    entry['制单日期'] = voucher_date
    entry['附单据数'] = 1
    entry['制单人'] = '1'
    entry['审核人'] = '4'
    entry['记账人'] = '4'
    entry['记账标志'] = 1
    entry['摘要'] = summary
    entry['科目编码'] = subject_code
    entry['借方金额'] = debit
    entry['贷方金额'] = credit
    entry['客户编码'] = customer_code
    entry['对方科目编码'] = counter_subject
    entry['银行帐两清标志'] = 0
    entry['往来帐两清标志'] = 0
    entry['是否核销'] = 0
    entry['日期'] = voucher_date
    entry['凭证是否可修改'] = 0
    entry['凭证分录是否可增删'] = 0
    entry['凭证合计金额是否保值'] = 0
    entry['分录数值是否可修改'] = 0
    entry['分录科目是否可修改'] = 0
    entry['分录受控科目可用状态'] = 0
    entry['分录往来项是否可修改'] = 0
    entry['分录部门是否可修改'] = 0
    entry['分录项目是否可修改'] = 0
    entry['分录往来项是否必输'] = 0
    entry['自定义字段1'] = customer_name

    return entry


def generate_voucher_entries(invoice_file: str, db_path: str, month: int, year: int = 2026,
                                voucher_no: int = 1, voucher_date: str = None) -> tuple:
    """
    生成凭证分录数据

    Args:
        invoice_file: 发票明细Excel文件
        db_path: SQLite数据库路径
        month: 月份 (1-12)
        year: 年份 (默认2026)
        voucher_no: 凭证编号
        voucher_date: 凭证日期 (格式: YYYY-MM-DD)

    Returns:
        (分录数据列表, 未匹配客户列表)
    """
    if voucher_date is None:
        voucher_date = f'{year}-{month:02d}-01'

    df = pd.read_excel(invoice_file, sheet_name='应收数据')
    print(f"读取应收数据: {len(df)} 个客户")

    entries = []
    row_no = 1
    unmatched_customers = []

    # 生成借方分录 (应收账款)
    for _, row in df.iterrows():
        customer_name = str(row['单位名称'])
        customer_code, matched = get_customer_code(db_path, customer_name)
        amount = float(row['单票合计'])
        amount_ex_tax = float(row['金额'])
        tax = float(row['税额'])

        if not matched:
            unmatched_customers.append({
                '单位名称': customer_name,
                '单票合计': amount,
                '金额': amount_ex_tax,
                '税额': tax
            })

        entry = create_entry(
            month=month, year=year, voucher_no=voucher_no, row_no=row_no,
            summary=f'{month}月应收账款', subject_code='122',
            debit=amount, credit=0,
            customer_code=customer_code, customer_name=customer_name,
            counter_subject='501,2210102', voucher_date=voucher_date
        )
        entries.append(entry)
        row_no += 1

    # 贷方分录1 (商品销售收入)
    total_income = float(df['金额'].sum())
    entry = create_entry(
        month=month, year=year, voucher_no=voucher_no, row_no=row_no,
        summary=f'{month}月销售收入', subject_code='501',
        debit=0, credit=total_income,
        customer_code='', customer_name='',
        counter_subject='', voucher_date=voucher_date
    )
    entries.append(entry)
    row_no += 1

    # 贷方分录2 (销项税)
    total_tax = float(df['税额'].sum())
    entry = create_entry(
        month=month, year=year, voucher_no=voucher_no, row_no=row_no,
        summary=f'{month}月销项税', subject_code='2210102',
        debit=0, credit=total_tax,
        customer_code='', customer_name='',
        counter_subject='', voucher_date=voucher_date
    )
    entries.append(entry)

    # 计算合计
    debit_total = sum(float(e['借方金额']) if e['借方金额'] else 0 for e in entries)
    credit_total = sum(float(e['贷方金额']) if e['贷方金额'] else 0 for e in entries)

    print(f"生成凭证分录: {len(entries)} 条")
    print(f"  借方合计: {debit_total:,.2f}")
    print(f"  贷方合计: {credit_total:,.2f}")

    if unmatched_customers:
        print(f"\n{'!' * 60}")
        print(f"警告: 发现 {len(unmatched_customers)} 个客户在客户档案中未找到匹配!")
        print(f"{'!' * 60}")
        print("\n未匹配客户列表:")
        for i, c in enumerate(unmatched_customers, 1):
            print(f"  {i}. {c['单位名称']} (金额: {c['单票合计']:,.2f})")
        print("\n请先维护客户档案后再生成凭证!")

    return entries, unmatched_customers


def save_to_excel(entries: list, output_file: str):
    """保存到Excel，严格按照模板字段顺序，金额保留两位小数"""
    from openpyxl import Workbook
    from openpyxl.styles import numbers

    df = pd.DataFrame(entries)
    # 确保列顺序与模板完全一致
    df = df[VOUCHER_FIELDS]

    # 使用openpyxl保存并设置格式
    wb = Workbook()
    ws = wb.active
    ws.title = 'Sheet1'

    # 写入标题行
    ws.append(list(df.columns))

    # 写入数据行
    for _, row in df.iterrows():
        ws.append(list(row))

    # 设置金额列格式 (两位小数)
    debit_col = list(df.columns).index('借方金额') + 1
    credit_col = list(df.columns).index('贷方金额') + 1

    for row in range(2, len(df) + 2):
        for col in [debit_col, credit_col]:
            cell = ws.cell(row=row, column=col)
            cell.number_format = '0.00'

    wb.save(output_file)

    print(f"\n已保存到: {output_file}")
    print(f"列数: {len(df.columns)} (模板要求: 73)")
    print(f"列名与模板一致: {list(df.columns) == VOUCHER_FIELDS}")


def export_unmatched_customers(unmatched: list, output_file: str):
    """导出未匹配客户列表"""
    if not unmatched:
        return
    df = pd.DataFrame(unmatched)
    df = df.sort_values('单票合计', ascending=False)
    df.to_excel(output_file, index=False, sheet_name='未匹配客户')
    print(f"未匹配客户已导出到: {output_file}")


def main():
    invoice_file = 'Docs/3月发票明细.xlsx'
    db_path = 'data.db'
    output_file = 'Docs/凭证分录_测试.xlsx'
    unmatched_file = 'Docs/未匹配客户列表.xlsx'
    month = 3
    year = 2026

    entries, unmatched = generate_voucher_entries(invoice_file, db_path, month, year)

    if entries is None:
        export_unmatched_customers(unmatched, unmatched_file)
        return

    save_to_excel(entries, output_file)

    print("\n前5条分录:")
    for e in entries[:5]:
        debit = float(e['借方金额']) if e['借方金额'] else 0
        credit = float(e['贷方金额']) if e['贷方金额'] else 0
        print(f"  行{e['行号']}: {e['科目编码']} {e['摘要'][:10]} 借={debit:>12,.2f} 贷={credit:>12,.2f}")

    print("\n最后3条分录:")
    for e in entries[-3:]:
        debit = float(e['借方金额']) if e['借方金额'] else 0
        credit = float(e['贷方金额']) if e['贷方金额'] else 0
        print(f"  行{e['行号']}: {e['科目编码']} {e['摘要'][:10]} 借={debit:>12,.2f} 贷={credit:>12,.2f}")


if __name__ == '__main__':
    main()
