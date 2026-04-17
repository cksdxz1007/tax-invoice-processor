#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
税务应收应付数据处理系统 - Web服务

使用Flask轻量化框架，提供网页界面
"""

import os
import sys
import uuid
import tempfile
from datetime import datetime

from flask import Flask, render_template, request, send_file, flash, redirect, url_for
import pandas as pd
import sqlite3

app = Flask(__name__)
app.secret_key = os.urandom(24)
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 最大上传100MB
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['OUTPUT_FOLDER'] = 'downloads'
app.config['SITE_TITLE'] = '税务应收应付数据处理系统'

# 确保目录存在
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['OUTPUT_FOLDER'], exist_ok=True)

# ============================================================================
# 凭证模板字段 (73个)
# ============================================================================
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

SECTION_MARKERS = {
    '专票': '本月开出专票',
    '普票': '本月开出普票',
    '进项': '本月收到进项',
}
SUMMARY_KEYWORDS = ['小计', '合计', '统计', '汇总']


# ============================================================================
# 核心处理函数
# ============================================================================
def find_section_boundaries(df):
    """动态识别分区边界"""
    boundaries = {}
    first_col = df.iloc[:, 0].astype(str)

    for section, marker in SECTION_MARKERS.items():
        matches = df[first_col.str.contains(marker, na=False)].index.tolist()
        if matches:
            boundaries[section] = {'start': matches[0]}

    sections = list(boundaries.keys())
    for i, section in enumerate(sections):
        if i < len(sections) - 1:
            next_section = sections[i + 1]
            boundaries[section]['end'] = boundaries[next_section]['start'] - 1
        else:
            last_data_row = boundaries[section]['start']
            for idx in range(boundaries[section]['start'] + 1, len(df)):
                row_val = df.iloc[idx, 0]
                if pd.isna(row_val) or any(kw in str(row_val) for kw in SUMMARY_KEYWORDS):
                    last_data_row = idx - 1
                    break
                last_data_row = idx
            boundaries[section]['end'] = last_data_row

    return boundaries


def find_column_row(df, start, end):
    """查找列名行"""
    for idx in range(start, min(end + 1, start + 5)):
        row_values = df.iloc[idx].astype(str).tolist()
        if '单位名称' in row_values and '日期' in row_values:
            return idx
    return start


def step1_split_invoice(invoice_file, month, output_file):
    """第一步: 拆分发票数据"""
    df = pd.read_excel(invoice_file, sheet_name=f'{month}月', header=None)
    boundaries = find_section_boundaries(df)

    sheets = {}
    for section, boundary in boundaries.items():
        col_row = find_column_row(df, boundary['start'], boundary['end'])
        data_start = col_row + 1
        section_df = df.iloc[data_start:boundary['end'] + 1].copy()
        section_df.columns = df.iloc[col_row].tolist()
        section_df = section_df.reset_index(drop=True)
        section_df = section_df[
            section_df['单位名称'].notna() &
            ~section_df['单位名称'].astype(str).str.contains('|'.join(SUMMARY_KEYWORDS), na=False)
        ]
        sheets[section] = section_df

    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        for section, df_section in sheets.items():
            df_section.to_excel(writer, sheet_name=section, index=False)

    return True, sheets


def step2_generate_receivable(input_file, month, output_file):
    """第二步: 生成应收数据"""
    df_vat = pd.read_excel(input_file, sheet_name='专票')
    df_normal = pd.read_excel(input_file, sheet_name='普票')
    df = pd.concat([df_vat, df_normal], ignore_index=True)

    receivable = df.groupby('单位名称').agg({
        '单票合计': 'sum',
        '金额': 'sum',
        '税额': 'sum',
        '发票张数': 'sum'
    }).reset_index()

    receivable.columns = ['单位名称', '单票合计', '金额', '税额', '发票张数']
    receivable = receivable.sort_values('单票合计', ascending=False).reset_index(drop=True)

    with pd.ExcelWriter(output_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        receivable.to_excel(writer, sheet_name='应收数据', index=False)

    return True, receivable


def get_customer_code(db_path, customer_name):
    """获取客户编码"""
    if not os.path.exists(db_path):
        return '', False
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


def create_entry(month, year, voucher_no, row_no, summary, subject_code, debit, credit,
                customer_code, customer_name, counter_subject, voucher_date):
    """创建单条分录"""
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


def step3_generate_voucher(invoice_file, db_path, month, year, voucher_no, output_file):
    """第三步: 生成凭证分录"""
    from openpyxl import Workbook

    voucher_date = f'{year}-{month:02d}-01'
    df = pd.read_excel(invoice_file, sheet_name='应收数据')

    entries = []
    row_no = 1
    unmatched_customers = []

    for _, row in df.iterrows():
        customer_name = str(row['单位名称'])
        customer_code, matched = get_customer_code(db_path, customer_name)
        single_total = float(row['单票合计'])
        amount = float(row['金额'])
        tax = float(row['税额'])

        if not matched:
            unmatched_customers.append({
                '单位名称': customer_name,
                '单票合计': single_total,
                '金额': amount,
                '税额': tax
            })

        # 分录1: 借方 应收账款
        entry = create_entry(
            month=month, year=year, voucher_no=voucher_no, row_no=row_no,
            summary=f'{month}月应收账款', subject_code='122',
            debit=single_total, credit=0,
            customer_code=customer_code, customer_name=customer_name,
            counter_subject='501,2210102', voucher_date=voucher_date
        )
        entries.append(entry)
        row_no += 1

        # 分录2: 贷方 商品销售收入
        entry = create_entry(
            month=month, year=year, voucher_no=voucher_no, row_no=row_no,
            summary=f'{month}月销售收入', subject_code='501',
            debit=0, credit=amount,
            customer_code=customer_code, customer_name=customer_name,
            counter_subject='122', voucher_date=voucher_date
        )
        entries.append(entry)
        row_no += 1

        # 分录3: 贷方 销项税
        entry = create_entry(
            month=month, year=year, voucher_no=voucher_no, row_no=row_no,
            summary=f'{month}月销项税', subject_code='2210102',
            debit=0, credit=tax,
            customer_code=customer_code, customer_name=customer_name,
            counter_subject='122', voucher_date=voucher_date
        )
        entries.append(entry)
        row_no += 1

    # 保存Excel
    df_out = pd.DataFrame(entries)
    df_out = df_out[VOUCHER_FIELDS]

    wb = Workbook()
    ws = wb.active
    ws.title = 'Sheet1'
    ws.append(list(df_out.columns))

    for _, row in df_out.iterrows():
        ws.append(list(row))

    # 设置金额格式
    debit_col = list(df_out.columns).index('借方金额') + 1
    credit_col = list(df_out.columns).index('贷方金额') + 1
    for row in range(2, len(df_out) + 2):
        for col in [debit_col, credit_col]:
            ws.cell(row=row, column=col).number_format = '0.00'

    wb.save(output_file)

    # 保存未匹配客户
    unmatched_file = output_file.replace('应收凭证分录.xlsx', '未匹配客户.xlsx')
    if unmatched_customers:
        df_unmatched = pd.DataFrame(unmatched_customers)
        df_unmatched = df_unmatched.sort_values('单票合计', ascending=False)
        df_unmatched.to_excel(unmatched_file, index=False, sheet_name='未匹配客户')

    return True, unmatched_customers


def process_invoice(invoice_file, target_month, year, db_path='data.db'):
    """执行全流程处理"""
    session_id = datetime.now().strftime('%Y%m%d%H%M%S')
    output_dir = os.path.join(app.config['OUTPUT_FOLDER'], session_id)
    os.makedirs(output_dir, exist_ok=True)

    results = {
        'success': True,
        'session_id': session_id,
        'files': [],
        'errors': [],
        'unmatched_count': 0
    }

    try:
        # 第一步
        invoice_detail_file = os.path.join(output_dir, f'{target_month}月发票明细.xlsx')
        success, sheets = step1_split_invoice(invoice_file, target_month, invoice_detail_file)
        if not success:
            raise Exception('第一步失败: 拆分发票数据')

        # 第二步
        success, receivable = step2_generate_receivable(invoice_detail_file, target_month, invoice_detail_file)
        if not success:
            raise Exception('第二步失败: 生成应收数据')

        # 第三步
        voucher_file = os.path.join(output_dir, f'{target_month}月应收凭证分录.xlsx')
        success, unmatched = step3_generate_voucher(
            invoice_detail_file, db_path, target_month, year, 1, voucher_file
        )
        if not success:
            raise Exception('第三步失败: 生成凭证分录')

        results['files'].append({
            'name': f'{target_month}月发票明细.xlsx',
            'path': invoice_detail_file,
            'display_name': '发票明细'
        })
        results['files'].append({
            'name': f'{target_month}月应收凭证分录.xlsx',
            'path': voucher_file,
            'display_name': '应收凭证分录'
        })
        results['unmatched_count'] = len(unmatched)

        if unmatched:
            results['files'].append({
                'name': f'{target_month}月未匹配客户.xlsx',
                'path': voucher_file.replace('应收凭证分录.xlsx', '未匹配客户.xlsx'),
                'display_name': '未匹配客户'
            })

    except Exception as e:
        results['success'] = False
        results['errors'].append(str(e))

    return results


# ============================================================================
# 路由
# ============================================================================
@app.route('/')
def index():
    """首页"""
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload():
    """处理上传"""
    if 'file' not in request.files:
        flash('请选择要上传的文件', 'error')
        return redirect(url_for('index'))

    file = request.files['file']
    if file.filename == '':
        flash('请选择要上传的文件', 'error')
        return redirect(url_for('index'))

    target_month = request.form.get('month', type=int)
    year = request.form.get('year', type=int, default=2026)

    if not target_month or target_month < 1 or target_month > 12:
        flash('请选择有效的月份 (1-12)', 'error')
        return redirect(url_for('index'))

    # 保存上传的文件
    filename = f'{uuid.uuid4().hex}_{file.filename}'
    upload_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(upload_path)

    try:
        # 检查工作表是否存在
        sheets = pd.ExcelFile(upload_path).sheet_names
        sheet_name = f'{target_month}月'
        if sheet_name not in sheets:
            os.remove(upload_path)
            flash(f"文件中未找到工作表 '{sheet_name}'，可用工作表: {', '.join(sheets)}", 'error')
            return redirect(url_for('index'))

        # 执行处理
        results = process_invoice(upload_path, target_month, year)

        # 清理上传的文件
        os.remove(upload_path)

        if results['success']:
            return render_template('download.html', results=results, month=target_month)
        else:
            flash('处理失败: ' + ', '.join(results['errors']), 'error')
            return redirect(url_for('index'))

    except Exception as e:
        if os.path.exists(upload_path):
            os.remove(upload_path)
        flash(f'处理出错: {str(e)}', 'error')
        return redirect(url_for('index'))


@app.route('/download/<session_id>/<filename>')
def download(session_id, filename):
    """下载文件"""
    file_path = os.path.join(app.config['OUTPUT_FOLDER'], session_id, filename)
    if not os.path.exists(file_path):
        flash('文件不存在', 'error')
        return redirect(url_for('index'))
    return send_file(file_path, as_attachment=True)


if __name__ == '__main__':
    print("=" * 60)
    print("税务应收应付数据处理系统 Web 服务")
    print("=" * 60)
    print("请访问: http://127.0.0.1:5000")
    print("按 Ctrl+C 停止服务")
    print("=" * 60)
    app.run(debug=True, host='127.0.0.1', port=5000)
