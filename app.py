#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
税务应收应付数据处理系统 - Web服务

仅负责处理上传文件和反馈结果的前端逻辑
数据处理逻辑由 run_full_pipeline.py 提供
档案管理逻辑由 archives.py 提供
"""

import os
import sys
import uuid
import tempfile
from datetime import datetime

from flask import Flask, render_template, request, send_file, flash, redirect, url_for
import pandas as pd

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from run_full_pipeline import (
    step1_split_invoice,
    step2_generate_receivable,
    step3_generate_voucher
)
from archives import ArchiveManager

app = Flask(__name__)
app.secret_key = os.urandom(24)
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['OUTPUT_FOLDER'] = 'downloads'
app.config['DB_PATH'] = 'data.db'

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['OUTPUT_FOLDER'], exist_ok=True)

archives = ArchiveManager(app.config['DB_PATH'])


# ============================================================================
# 主页
# ============================================================================
@app.route('/')
def index():
    return render_template('index.html')


# ============================================================================
# 文件处理
# ============================================================================
@app.route('/upload', methods=['POST'])
def upload():
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

    filename = f'{uuid.uuid4().hex}_{file.filename}'
    upload_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(upload_path)

    try:
        sheets = pd.ExcelFile(upload_path).sheet_names
        sheet_name = f'{target_month}月'
        if sheet_name not in sheets:
            os.remove(upload_path)
            flash(f"文件中未找到工作表 '{sheet_name}'，可用: {', '.join(sheets)}", 'error')
            return redirect(url_for('index'))

        session_id = datetime.now().strftime('%Y%m%d%H%M%S')
        output_dir = os.path.join(app.config['OUTPUT_FOLDER'], session_id)
        os.makedirs(output_dir, exist_ok=True)

        invoice_detail_file = os.path.join(output_dir, f'{target_month}月发票明细.xlsx')
        voucher_file = os.path.join(output_dir, f'{target_month}月应收凭证分录.xlsx')
        unmatched_file = os.path.join(output_dir, f'{target_month}月未匹配客户.xlsx')

        step1_split_invoice(upload_path, target_month, invoice_detail_file)
        _, data = step2_generate_receivable(invoice_detail_file, target_month, invoice_detail_file)

        receivable_count = len(data.get('receivable', []))
        payable_count = len(data.get('payable', []))

        _, unmatched = step3_generate_voucher(
            invoice_detail_file, app.config['DB_PATH'], target_month, year, 1,
            voucher_file, unmatched_file
        )

        results = {
            'success': True,
            'session_id': session_id,
            'files': [
                {'name': f'{target_month}月发票明细.xlsx', 'path': invoice_detail_file,
                 'display_name': '发票明细 (含应收/应付数据)'},
                {'name': f'{target_month}月应收凭证分录.xlsx', 'path': voucher_file,
                 'display_name': '应收凭证分录'},
            ],
            'unmatched_count': len(unmatched),
            'receivable_count': receivable_count,
            'payable_count': payable_count
        }

        if unmatched:
            results['files'].append({
                'name': f'{target_month}月未匹配客户.xlsx',
                'path': unmatched_file,
                'display_name': '未匹配客户'
            })

        os.remove(upload_path)
        return render_template('download.html', results=results, month=target_month)

    except Exception as e:
        if os.path.exists(upload_path):
            os.remove(upload_path)
        flash(f'处理出错: {str(e)}', 'error')
        return redirect(url_for('index'))


@app.route('/download/<session_id>/<filename>')
def download(session_id, filename):
    file_path = os.path.join(app.config['OUTPUT_FOLDER'], session_id, filename)
    if not os.path.exists(file_path):
        flash('文件不存在', 'error')
        return redirect(url_for('index'))
    return send_file(file_path, as_attachment=True)


# ============================================================================
# 档案管理
# ============================================================================
@app.route('/manage')
def manage():
    customers = archives.get_customers()
    suppliers = archives.get_suppliers()
    return render_template('manage.html', customers=customers, suppliers=suppliers)


@app.route('/api/customer/<code>')
def get_customer(code):
    return archives.get_customer(code)


@app.route('/api/supplier/<code>')
def get_supplier(code):
    return archives.get_supplier(code)


@app.route('/add_customer', methods=['POST'])
def add_customer():
    success, msg = archives.save_customer(
        request.form.get('客户编号'),
        request.form.get('客户名称'),
        request.form.get('客户简称', ''),
        request.form.get('总公司全称', '')
    )
    flash(msg, 'success' if success else 'error')
    return redirect(url_for('manage'))


@app.route('/add_supplier', methods=['POST'])
def add_supplier():
    success, msg = archives.save_supplier(
        request.form.get('供应商编号'),
        request.form.get('供应商名称'),
        request.form.get('供应商简称', '')
    )
    flash(msg, 'success' if success else 'error')
    return redirect(url_for('manage'))


@app.route('/export_customers')
def export_customers():
    file_path = archives.export_customers()
    return send_file(file_path, as_attachment=True, download_name='客户档案.xlsx')


@app.route('/export_suppliers')
def export_suppliers():
    file_path = archives.export_suppliers()
    return send_file(file_path, as_attachment=True, download_name='供应商档案.xlsx')


@app.route('/download_customer_template')
def download_customer_template():
    file_path = archives.download_customer_template()
    return send_file(file_path, as_attachment=True, download_name='客户档案导入模板.xlsx')


@app.route('/download_supplier_template')
def download_supplier_template():
    file_path = archives.download_supplier_template()
    return send_file(file_path, as_attachment=True, download_name='供应商档案导入模板.xlsx')


@app.route('/import_customers', methods=['POST'])
def import_customers():
    if 'file' not in request.files:
        flash('请选择要上传的文件', 'error')
        return redirect(url_for('manage'))

    file = request.files['file']
    if file.filename == '':
        flash('请选择要上传的文件', 'error')
        return redirect(url_for('manage'))

    upload_path = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False).name
    file.save(upload_path)

    success, msg, _, _ = archives.import_customers(upload_path)
    os.remove(upload_path)

    flash(msg, 'success' if success else 'error')
    return redirect(url_for('manage'))


@app.route('/import_suppliers', methods=['POST'])
def import_suppliers():
    if 'file' not in request.files:
        flash('请选择要上传的文件', 'error')
        return redirect(url_for('manage'))

    file = request.files['file']
    if file.filename == '':
        flash('请选择要上传的文件', 'error')
        return redirect(url_for('manage'))

    upload_path = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False).name
    file.save(upload_path)

    success, msg, _, _ = archives.import_suppliers(upload_path)
    os.remove(upload_path)

    flash(msg, 'success' if success else 'error')
    return redirect(url_for('manage'))


# ============================================================================
# 启动
# ============================================================================
if __name__ == '__main__':
    print("=" * 60)
    print("税务应收应付数据处理系统 Web 服务")
    print("=" * 60)
    print("请访问: http://127.0.0.1:5000")
    print("按 Ctrl+C 停止服务")
    print("=" * 60)
    app.run(debug=True, host='127.0.0.1', port=5000)
