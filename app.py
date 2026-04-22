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
from functools import wraps

from flask import Flask, render_template, request, send_file, flash, redirect, url_for, session
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
app.config['HTPASSWD_FILE'] = '.htpasswd'

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['OUTPUT_FOLDER'], exist_ok=True)

archives = ArchiveManager(app.config['DB_PATH'])


# ============================================================================
# 认证功能
# ============================================================================
def check_auth(username, password):
    """验证用户名和密码"""
    import hashlib

    htpasswd_file = app.config['HTPASSWD_FILE']
    if not os.path.exists(htpasswd_file):
        return False

    # 计算密码的 SHA256 哈希
    password_hash = hashlib.sha256(password.encode()).hexdigest()

    with open(htpasswd_file, 'r') as f:
        for line in f:
            parts = line.strip().split(':')
            if len(parts) >= 2 and parts[0] == username:
                stored_hash = parts[1]
                return password_hash == stored_hash
    return False


def authenticate():
    """发送认证失败响应"""
    flash('请先登录', 'error')
    return redirect(url_for('login'))


def requires_auth(f):
    """登录验证装饰器"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'authenticated' not in session:
            return authenticate()
        return f(*args, **kwargs)
    return decorated_function


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username', '')
        password = request.form.get('password', '')

        if check_auth(username, password):
            session['authenticated'] = True
            session['username'] = username
            flash('登录成功', 'success')
            return redirect(url_for('index'))
        else:
            flash('用户名或密码错误', 'error')

    return render_template('login.html')


@app.route('/logout')
def logout():
    session.pop('authenticated', None)
    session.pop('username', None)
    flash('已退出登录', 'success')
    return redirect(url_for('login'))


# ============================================================================
# 主页
# ============================================================================
@app.route('/')
@requires_auth
def index():
    return render_template('index.html')


# ============================================================================
# 文件处理
# ============================================================================
@app.route('/upload', methods=['POST'])
@requires_auth
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
        step2_generate_receivable(invoice_detail_file, target_month, invoice_detail_file)

        # 从生成的Excel文件中读取应收/应付数据数量
        df_receivable = pd.read_excel(invoice_detail_file, sheet_name='应收数据')
        df_payable = pd.read_excel(invoice_detail_file, sheet_name='应付数据')
        receivable_count = len(df_receivable[df_receivable['单位名称'] != '合计'])
        payable_count = len(df_payable[df_payable['单位名称'] != '合计'])

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
@requires_auth
def download(session_id, filename):
    file_path = os.path.join(app.config['OUTPUT_FOLDER'], session_id, filename)
    if not os.path.exists(file_path):
        flash('文件不存在', 'error')
        return redirect(url_for('index'))
    return send_file(file_path, as_attachment=True)


@app.route('/cleanup', methods=['POST'])
@requires_auth
def cleanup():
    """清理上传和处理结果文件"""
    try:
        import shutil

        folders = [
            app.config['UPLOAD_FOLDER'],
            app.config['OUTPUT_FOLDER']
        ]

        cleaned_count = 0
        for folder in folders:
            if os.path.exists(folder):
                for item in os.listdir(folder):
                    item_path = os.path.join(folder, item)
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                        cleaned_count += 1
                    elif os.path.isfile(item_path):
                        os.remove(item_path)
                        cleaned_count += 1

        flash(f'已清理 {cleaned_count} 个文件/目录', 'success')
    except Exception as e:
        flash(f'清理失败: {str(e)}', 'error')

    return redirect(url_for('index'))


@app.route('/files')
@requires_auth
def list_files():
    """列出历史文件"""
    files = []

    output_folder = app.config['OUTPUT_FOLDER']
    if os.path.exists(output_folder):
        for session_id in os.listdir(output_folder):
            session_path = os.path.join(output_folder, session_id)
            if os.path.isdir(session_path):
                for filename in os.listdir(session_path):
                    file_path = os.path.join(session_path, filename)
                    stat = os.stat(file_path)
                    files.append({
                        'session_id': session_id,
                        'filename': filename,
                        'size': stat.st_size,
                        'mtime': datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                        'path': file_path
                    })

    files.sort(key=lambda x: x['mtime'], reverse=True)
    return render_template('files.html', files=files)


# ============================================================================
# 档案管理
# ============================================================================
@app.route('/manage')
@requires_auth
def manage():
    customers = archives.get_customers()
    suppliers = archives.get_suppliers()
    return render_template('manage.html', customers=customers, suppliers=suppliers)


@app.route('/api/customer/<code>')
@requires_auth
def get_customer(code):
    return archives.get_customer(code)


@app.route('/api/supplier/<code>')
@requires_auth
def get_supplier(code):
    return archives.get_supplier(code)


@app.route('/add_customer', methods=['POST'])
@requires_auth
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
@requires_auth
def add_supplier():
    success, msg = archives.save_supplier(
        request.form.get('供应商编号'),
        request.form.get('供应商名称'),
        request.form.get('供应商简称', '')
    )
    flash(msg, 'success' if success else 'error')
    return redirect(url_for('manage'))


@app.route('/export_customers')
@requires_auth
def export_customers():
    file_path = archives.export_customers()
    return send_file(file_path, as_attachment=True, download_name='客户档案.xlsx')


@app.route('/export_suppliers')
@requires_auth
def export_suppliers():
    file_path = archives.export_suppliers()
    return send_file(file_path, as_attachment=True, download_name='供应商档案.xlsx')


@app.route('/download_customer_template')
@requires_auth
def download_customer_template():
    file_path = archives.download_customer_template()
    return send_file(file_path, as_attachment=True, download_name='客户档案导入模板.xlsx')


@app.route('/download_supplier_template')
@requires_auth
def download_supplier_template():
    file_path = archives.download_supplier_template()
    return send_file(file_path, as_attachment=True, download_name='供应商档案导入模板.xlsx')


@app.route('/import_customers', methods=['POST'])
@requires_auth
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
@requires_auth
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
    import os
    port = int(os.environ.get('PORT', 5000))

    print("=" * 60)
    print("税务应收应付数据处理系统 Web 服务")
    print("=" * 60)
    print(f"请访问: http://127.0.0.1:{port}")
    print("按 Ctrl+C 停止服务")
    print("=" * 60)
    app.run(debug=True, host='0.0.0.0', port=port)
