#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
档案管理模块

提供客户档案和供应商档案的增删改查、导入导出功能
"""

import os
import tempfile
import pandas as pd
import sqlite3


class ArchiveManager:
    """档案管理器"""

    def __init__(self, db_path='data.db'):
        self.db_path = db_path

    def _get_connection(self):
        """获取数据库连接"""
        conn = sqlite3.connect(self.db_path)
        return conn

    # =========================================================================
    # 客户档案
    # =========================================================================
    def get_customers(self):
        """获取所有客户档案"""
        conn = self._get_connection()
        cursor = conn.execute(
            'SELECT 客户编号, 客户名称, 客户简称, 总公司全称 FROM customers ORDER BY 客户编号'
        )
        customers = [tuple(row) for row in cursor.fetchall()]
        conn.close()
        return customers

    def get_customer(self, code):
        """获取单个客户档案"""
        conn = self._get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.execute('SELECT * FROM customers WHERE 客户编号 = ?', (code,))
        customer = cursor.fetchone()
        conn.close()
        return dict(customer) if customer else None

    def save_customer(self, code, name, short='', parent=''):
        """
        新增或更新客户档案

        Returns:
            tuple: (success: bool, message: str)
        """
        conn = self._get_connection()
        try:
            cursor = conn.execute('SELECT COUNT(*) FROM customers WHERE 客户编号 = ?', (code,))
            exists = cursor.fetchone()[0] > 0

            if exists:
                conn.execute('''
                    UPDATE customers
                    SET 客户名称 = ?, 客户简称 = ?, 总公司全称 = ?
                    WHERE 客户编号 = ?
                ''', (name, short, parent, code))
                conn.commit()
                conn.close()
                return True, f'客户 {name} 已更新'
            else:
                conn.execute('''
                    INSERT INTO customers (客户编号, 客户名称, 客户简称, 总公司全称)
                    VALUES (?, ?, ?, ?)
                ''', (code, name, short, parent))
                conn.commit()
                conn.close()
                return True, f'客户 {name} 已添加'

        except Exception as e:
            conn.close()
            return False, str(e)

    def export_customers(self):
        """
        导出所有客户档案为Excel

        Returns:
            str: 临时文件路径
        """
        conn = self._get_connection()
        df = pd.read_sql_query('SELECT * FROM customers ORDER BY 客户编号', conn)
        conn.close()

        output = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
        df.to_excel(output, index=False, sheet_name='客户档案')
        output.close()

        return output.name

    def import_customers(self, file_path):
        """
        导入客户档案

        Args:
            file_path: Excel或CSV文件路径

        Returns:
            tuple: (success: bool, message: str, imported: int, updated: int)
        """
        try:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            else:
                df = pd.read_excel(file_path)

            required_cols = ['客户编号', '客户名称']
            missing = [col for col in required_cols if col not in df.columns]
            if missing:
                return False, f'缺少必要的列: {", ".join(missing)}', 0, 0

            conn = self._get_connection()
            imported = updated = 0

            for _, row in df.iterrows():
                code = str(row['客户编号']).strip()
                name = str(row['客户名称']).strip()
                short = str(row.get('客户简称', '')).strip() if pd.notna(row.get('客户简称')) else ''
                parent = str(row.get('总公司全称', '')).strip() if pd.notna(row.get('总公司全称')) else ''

                if not code or not name:
                    continue

                cursor = conn.execute('SELECT COUNT(*) FROM customers WHERE 客户编号 = ?', (code,))
                if cursor.fetchone()[0] > 0:
                    conn.execute('''
                        UPDATE customers
                        SET 客户名称 = ?, 客户简称 = ?, 总公司全称 = ?
                        WHERE 客户编号 = ?
                    ''', (name, short, parent, code))
                    updated += 1
                else:
                    conn.execute('''
                        INSERT INTO customers (客户编号, 客户名称, 客户简称, 总公司全称)
                        VALUES (?, ?, ?, ?)
                    ''', (code, name, short, parent))
                    imported += 1

            conn.commit()
            conn.close()

            return True, f'导入完成: 新增 {imported} 条, 更新 {updated} 条', imported, updated

        except Exception as e:
            return False, f'导入失败: {str(e)}', 0, 0

    def download_customer_template(self):
        """
        下载客户档案导入模板

        Returns:
            str: 临时文件路径
        """
        df = pd.DataFrame(columns=['客户编号', '客户名称', '客户简称', '总公司全称'])
        output = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
        df.to_excel(output, index=False, sheet_name='客户档案模板')
        output.close()
        return output.name

    # =========================================================================
    # 供应商档案
    # =========================================================================
    def get_suppliers(self):
        """获取所有供应商档案"""
        conn = self._get_connection()
        cursor = conn.execute(
            'SELECT 供应商编号, 供应商名称, 供应商简称 FROM suppliers ORDER BY 供应商编号'
        )
        suppliers = [tuple(row) for row in cursor.fetchall()]
        conn.close()
        return suppliers

    def get_supplier(self, code):
        """获取单个供应商档案"""
        conn = self._get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.execute('SELECT * FROM suppliers WHERE 供应商编号 = ?', (code,))
        supplier = cursor.fetchone()
        conn.close()
        return dict(supplier) if supplier else None

    def save_supplier(self, code, name, short=''):
        """
        新增或更新供应商档案

        Returns:
            tuple: (success: bool, message: str)
        """
        conn = self._get_connection()
        try:
            cursor = conn.execute('SELECT COUNT(*) FROM suppliers WHERE 供应商编号 = ?', (code,))
            exists = cursor.fetchone()[0] > 0

            if exists:
                conn.execute('''
                    UPDATE suppliers
                    SET 供应商名称 = ?, 供应商简称 = ?
                    WHERE 供应商编号 = ?
                ''', (name, short, code))
                conn.commit()
                conn.close()
                return True, f'供应商 {name} 已更新'
            else:
                conn.execute('''
                    INSERT INTO suppliers (供应商编号, 供应商名称, 供应商简称)
                    VALUES (?, ?, ?)
                ''', (code, name, short))
                conn.commit()
                conn.close()
                return True, f'供应商 {name} 已添加'

        except Exception as e:
            conn.close()
            return False, str(e)

    def export_suppliers(self):
        """
        导出所有供应商档案为Excel

        Returns:
            str: 临时文件路径
        """
        conn = self._get_connection()
        df = pd.read_sql_query('SELECT * FROM suppliers ORDER BY 供应商编号', conn)
        conn.close()

        output = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
        df.to_excel(output, index=False, sheet_name='供应商档案')
        output.close()

        return output.name

    def import_suppliers(self, file_path):
        """
        导入供应商档案

        Args:
            file_path: Excel或CSV文件路径

        Returns:
            tuple: (success: bool, message: str, imported: int, updated: int)
        """
        try:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            else:
                df = pd.read_excel(file_path)

            required_cols = ['供应商编号', '供应商名称']
            missing = [col for col in required_cols if col not in df.columns]
            if missing:
                return False, f'缺少必要的列: {", ".join(missing)}', 0, 0

            conn = self._get_connection()
            imported = updated = 0

            for _, row in df.iterrows():
                code = str(row['供应商编号']).strip()
                name = str(row['供应商名称']).strip()
                short = str(row.get('供应商简称', '')).strip() if pd.notna(row.get('供应商简称')) else ''

                if not code or not name:
                    continue

                cursor = conn.execute('SELECT COUNT(*) FROM suppliers WHERE 供应商编号 = ?', (code,))
                if cursor.fetchone()[0] > 0:
                    conn.execute('''
                        UPDATE suppliers
                        SET 供应商名称 = ?, 供应商简称 = ?
                        WHERE 供应商编号 = ?
                    ''', (name, short, code))
                    updated += 1
                else:
                    conn.execute('''
                        INSERT INTO suppliers (供应商编号, 供应商名称, 供应商简称)
                        VALUES (?, ?, ?)
                    ''', (code, name, short))
                    imported += 1

            conn.commit()
            conn.close()

            return True, f'导入完成: 新增 {imported} 条, 更新 {updated} 条', imported, updated

        except Exception as e:
            return False, f'导入失败: {str(e)}', 0, 0

    def download_supplier_template(self):
        """
        下载供应商档案导入模板

        Returns:
            str: 临时文件路径
        """
        df = pd.DataFrame(columns=['供应商编号', '供应商名称', '供应商简称'])
        output = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
        df.to_excel(output, index=False, sheet_name='供应商档案模板')
        output.close()
        return output.name
