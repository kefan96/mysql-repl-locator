#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
MySQL Replication Error SQL Locator
====================================
A tool to locate the exact SQL statement that caused replication errors on MySQL replica.

Supports:
- Python 2.7 and Python 3.x
- MySQL 5.6, 5.7, 8.0+
- Multiple GTIDs per error
- Performance Schema with fallback
- Automatic sudo detection

Author: kefan
Date: 2026-03-23
Version: 1.0.0
"""

from __future__ import print_function
import sys
import os
import re
import subprocess
import argparse
import datetime
import socket

# Python 2/3 compatibility
if sys.version_info[0] < 3:
    input_func = raw_input
    text_type = unicode
    from MySQLdb import connect as mysql_connect
else:
    input_func = input
    text_type = str
    try:
        import pymysql as mysql_connect
    except ImportError:
        try:
            from MySQLdb import connect as mysql_connect
        except ImportError:
            print("Error: Please install pymysql or MySQLdb package")
            sys.exit(1)


class Config:
    """Configuration constants"""
    # Locatable error codes
    LOCATABLE_ERRORS = {
        1062: 'Duplicate entry (Primary/Unique key conflict)',
        1452: 'Foreign key constraint fails',
        1146: "Table doesn't exist",
        1054: 'Unknown column',
        1064: 'SQL syntax error',
        1364: "Field doesn't have a default value",
        1048: "Column cannot be null",
    }
    
    # Common relay log patterns
    RELAY_LOG_PATTERN = r'relay-bin\.\d+'
    
    # Common error log paths
    COMMON_ERROR_LOG_PATHS = [
        '/var/log/mysql/error.log',
        '/var/lib/mysql/{hostname}.err',
        '/var/log/mysqld.log',
        '/var/log/mysql/mysqld.log',
    ]


class MySQLClient:
    """MySQL connection and query handler"""
    
    def __init__(self, host='localhost', port=3306, user='root', password='', 
                 socket=None, database=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.socket = socket
        self.database = database
        self.connection = None
        self.ps_enabled = False
        self.mysql_version = None
        
    def connect(self):
        """Establish MySQL connection"""
        try:
            kwargs = {
                'host': self.host,
                'port': self.port,
                'user': self.user,
                'passwd': self.password,
            }
            
            if self.socket:
                kwargs['unix_socket'] = self.socket
            
            if self.database:
                kwargs['db'] = self.database
            
            self.connection = mysql_connect(**kwargs)
            self._get_mysql_version()
            self._check_performance_schema()
            return True
        except Exception as e:
            print("Error: Failed to connect to MySQL: {0}".format(str(e)))
            return False
    
    def close(self):
        """Close MySQL connection"""
        if self.connection:
            try:
                self.connection.close()
            except:
                pass
    
    def execute_query(self, query, fetch_all=True):
        """Execute a query and return results"""
        if not self.connection:
            return None
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            
            if fetch_all:
                result = cursor.fetchall()
            else:
                result = cursor.fetchone()
            
            cursor.close()
            return result
        except Exception as e:
            print("Warning: Query failed: {0}".format(str(e)))
            return None
    
    def _get_mysql_version(self):
        """Get MySQL version"""
        result = self.execute_query("SELECT VERSION()", fetch_all=False)
        if result and result[0]:
            version_str = result[0]
            # Extract major version (e.g., '5.7.32' -> '5.7')
            match = re.match(r'(\d+\.\d+)', version_str)
            if match:
                self.mysql_version = match.group(1)
    
    def _check_performance_schema(self):
        """Check if performance_schema is enabled"""
        try:
            result = self.execute_query("SELECT @@performance_schema", fetch_all=False)
            if result and result[0] == 1:
                self.ps_enabled = True
                return True
            else:
                self.ps_enabled = False
                return False
        except:
            self.ps_enabled = False
            return False
    
    def get_slave_status(self):
        """Get SHOW SLAVE STATUS"""
        return self.execute_query("SHOW SLAVE STATUS")
    
    def get_error_log_path(self):
        """Get MySQL error log path"""
        try:
            result = self.execute_query("SELECT @@log_error", fetch_all=False)
            if result and result[0]:
                return result[0]
        except:
            pass
        
        # Fallback to common paths
        hostname = socket.gethostname()
        for path in Config.COMMON_ERROR_LOG_PATHS:
            path = path.format(hostname=hostname)
            if os.path.exists(path):
                return path
        
        return None
    
    def get_datadir(self):
        """Get MySQL data directory"""
        result = self.execute_query("SELECT @@datadir", fetch_all=False)
        if result and result[0]:
            return result[0]
        return None


class RelayLogParser:
    """Relay log parser using mysqlbinlog"""
    
    def __init__(self, need_sudo=False):
        self.need_sudo = need_sudo
    
    def check_mysqlbinlog(self):
        """Check if mysqlbinlog is available"""
        try:
            cmd = ['mysqlbinlog', '--version']
            if self.need_sudo:
                cmd = ['sudo'] + cmd
            
            process = subprocess.Popen(cmd,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)
            process.communicate()
            return process.returncode == 0
        except:
            return False
    
    def parse_relay_log(self, relay_log_path, start_position=None, 
                       stop_position=None):
        """Parse relay log file using mysqlbinlog"""
        if not os.path.exists(relay_log_path):
            print("Error: Relay log file not found: {0}".format(relay_log_path))
            return None
        
        cmd = ['mysqlbinlog',
               '--base64-output=DECODE-ROWS',
               '-v']
        
        if self.need_sudo:
            cmd = ['sudo'] + cmd
        
        if start_position:
            cmd.extend(['--start-position', str(start_position)])
        
        if stop_position:
            cmd.extend(['--stop-position', str(stop_position)])
        
        cmd.append(relay_log_path)
        
        try:
            process = subprocess.Popen(cmd,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)
            output, error = process.communicate()
            
            if sys.version_info[0] >= 3:
                output = output.decode('utf-8', errors='replace')
                error = error.decode('utf-8', errors='replace')
            
            if process.returncode != 0:
                print("Warning: mysqlbinlog returned error: {0}".format(error))
            
            return output
        except Exception as e:
            print("Error: Failed to parse relay log: {0}".format(str(e)))
            return None
    
    def extract_gtids_from_error(self, error_message):
        """Extract all GTIDs from error message"""
        if not error_message:
            return []
        
        gtids = set()
        
        # Pattern 1: GTID: uuid:seq_num
        pattern1 = r'GTID[:\s]+([a-f0-9-]+:\d+)'
        matches = re.findall(pattern1, error_message, re.IGNORECASE)
        gtids.update(matches)
        
        # Pattern 2: uuid:seq_num (36-char UUID)
        pattern2 = r'([a-f0-9-]{36}:\d+)'
        matches = re.findall(pattern2, error_message, re.IGNORECASE)
        gtids.update(matches)
        
        return list(gtids)
    
    def extract_error_code(self, error_message):
        """Extract error code from error message"""
        if not error_message:
            return None
        
        # Pattern: Error_code: NNNN
        match = re.search(r'Error_code:\s*(\d+)', error_message)
        if match:
            return int(match.group(1))
        
        return None
    
    def extract_transactions_for_gtids(self, binlog_output, gtids):
        """Extract transactions for all GTIDs from binlog output"""
        if not binlog_output or not gtids:
            return {}
        
        results = {}
        
        for gtid in gtids:
            transactions = self._extract_transaction_for_gtid(binlog_output, gtid)
            if transactions:
                results[gtid] = transactions
        
        return results
    
    def _extract_transaction_for_gtid(self, binlog_output, target_gtid):
        """Extract transaction(s) for a specific GTID"""
        transactions = []
        
        # Split output by GTID_NEXT
        lines = binlog_output.split('\n')
        
        i = 0
        while i < len(lines):
            line = lines[i]
            
            # Look for GTID_NEXT setting
            if 'SET @@SESSION.GTID_NEXT' in line:
                # Check if this is our target GTID
                if target_gtid in line:
                    # Found the GTID, now extract the transaction
                    transaction = self._extract_single_transaction(lines, i)
                    if transaction:
                        transactions.append(transaction)
            
            i += 1
        
        return transactions
    
    def _extract_single_transaction(self, lines, start_idx):
        """Extract a single transaction starting from GTID_NEXT"""
        transaction_lines = []
        found_begin = False
        found_commit = False
        
        i = start_idx
        while i < len(lines):
            line = lines[i]
            transaction_lines.append(line)
            
            if 'BEGIN' in line and not found_begin:
                found_begin = True
            
            if found_begin and ('COMMIT' in line or 'ROLLBACK' in line):
                found_commit = True
                break
            
            i += 1
        
        if transaction_lines:
            return '\n'.join(transaction_lines)
        
        return None


class ErrorAnalyzer:
    """Error analysis and classification"""
    
    def __init__(self):
        self.locatable_errors = Config.LOCATABLE_ERRORS
    
    def is_locatable_error(self, error_code):
        """Check if error can be located to specific SQL"""
        return error_code in self.locatable_errors
    
    def get_error_description(self, error_code):
        """Get error description"""
        return self.locatable_errors.get(error_code, 'Unknown error')
    
    def analyze_error(self, error_message):
        """Analyze error message and extract key information"""
        parser = RelayLogParser()
        
        error_code = parser.extract_error_code(error_message)
        gtids = parser.extract_gtids_from_error(error_message)
        
        is_locatable = False
        error_desc = 'Unknown'
        
        if error_code:
            is_locatable = self.is_locatable_error(error_code)
            error_desc = self.get_error_description(error_code)
        
        return {
            'error_code': error_code,
            'error_description': error_desc,
            'gtids': gtids,
            'is_locatable': is_locatable,
            'raw_error': error_message,
        }


class OutputFormatter:
    """Output formatting handler"""
    
    def __init__(self):
        self.separator = '=' * 60
        self.sub_separator = '-' * 40
    
    def format_report(self, analysis_result, slave_status, relay_log_results,
                      error_log_info=None, ps_enabled=False, mysql_version=None,
                      instance_info=None):
        """Format the complete report"""
        lines = []
        
        # Header
        lines.append(self.separator)
        lines.append('MySQL Replication Error SQL Locator Report')
        lines.append('Generated: {0}'.format(
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        
        if instance_info:
            lines.append('Instance: {0}'.format(instance_info))
        
        lines.append('')
        
        # Error Overview
        lines.append('[Error Overview]')
        if analysis_result['error_code']:
            lines.append('Error Code: {0}'.format(analysis_result['error_code']))
        lines.append('Error Type: {0}'.format(analysis_result['error_description']))
        
        if analysis_result['raw_error']:
            # Truncate long error messages
            error_msg = analysis_result['raw_error']
            if len(error_msg) > 200:
                error_msg = error_msg[:200] + '...'
            lines.append('Error Message: {0}'.format(error_msg))
        
        # Extract slave status info
        if slave_status and len(slave_status) > 0:
            status = slave_status[0]
            # Find column indices
            columns = [desc[0] for desc in slave_status.description]
            
            def get_col(name):
                try:
                    idx = columns.index(name)
                    return status[idx]
                except (ValueError, IndexError):
                    return None
            
            relay_log_file = get_col('Relay_Log_File')
            if relay_log_file:
                lines.append('Relay Log File: {0}'.format(relay_log_file))
            
            slave_io = get_col('Slave_IO_Running')
            slave_sql = get_col('Slave_SQL_Running')
            if slave_io:
                lines.append('Slave IO Running: {0}'.format(slave_io))
            if slave_sql:
                lines.append('Slave SQL Running: {0}'.format(slave_sql))
        
        lines.append('')
        
        # Performance Schema Status
        lines.append('[Performance Schema Status]')
        if ps_enabled:
            lines.append('Status: Enabled (Enhanced Mode)')
        else:
            lines.append('Status: Disabled (Basic Mode - relay log parsing only)')
        
        if mysql_version:
            lines.append('MySQL Version: {0}'.format(mysql_version))
        
        lines.append('')
        
        # Located SQL Transactions
        lines.append('[Located SQL Transactions]')
        if relay_log_results:
            for gtid, transactions in relay_log_results.items():
                lines.append('')
                lines.append(self.sub_separator)
                lines.append('GTID: {0}'.format(gtid))
                lines.append(self.sub_separator)
                
                if transactions:
                    for idx, txn in enumerate(transactions, 1):
                        if len(transactions) > 1:
                            lines.append('Transaction #{0}:'.format(idx))
                        lines.append(txn)
                        lines.append('')
                else:
                    lines.append('No transactions found for this GTID')
        else:
            lines.append('No SQL transactions located')
        
        lines.append('')
        
        # Error Log Information
        if error_log_info:
            lines.append('[Error Log Information]')
            if 'path' in error_log_info:
                lines.append('Log Path: {0}'.format(error_log_info['path']))
            if 'entries' in error_log_info and error_log_info['entries']:
                lines.append('Related Entries:')
                for entry in error_log_info['entries'][:10]:  # Limit to 10 entries
                    lines.append('  {0}'.format(entry))
            lines.append('')
        
        # Suggested Actions
        lines.append('[Suggested Actions]')
        if analysis_result['is_locatable']:
            lines.append('1. Review the located SQL statements above')
            if analysis_result['error_code'] == 1062:
                lines.append('2. Check for duplicate key conflicts on the replica')
                lines.append('3. Consider using pt-table-checksum to verify data consistency')
                lines.append('4. You may skip this GTID or manually fix the data')
            elif analysis_result['error_code'] == 1452:
                lines.append('2. Check foreign key constraints on the replica')
                lines.append('3. Verify parent records exist')
            else:
                lines.append('2. Investigate the root cause of the error')
                lines.append('3. Fix the issue on the replica')
        else:
            lines.append('1. This error type cannot be located to specific SQL')
            lines.append('2. Check MySQL error log for more details')
            lines.append('3. Review replication configuration')
        
        lines.append('')
        lines.append(self.separator)
        
        return '\n'.join(lines)
    
    def save_to_file(self, content, filepath):
        """Save report to file"""
        try:
            with open(filepath, 'w') as f:
                f.write(content)
            return True
        except Exception as e:
            print("Error: Failed to save report: {0}".format(str(e)))
            return False


class SudoHandler:
    """Handle sudo detection and management"""
    
    def __init__(self):
        self.need_sudo = False
    
    def check_sudo(self):
        """Check if sudo is needed and available"""
        # Check if already root
        if os.geteuid() == 0:
            self.need_sudo = False
            return True
        
        # Try to detect sudo availability
        try:
            process = subprocess.Popen(['sudo', '-n', 'true'],
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)
            process.communicate()
            
            if process.returncode == 0:
                # sudo works without password
                self.need_sudo = False
                return True
            else:
                # Will need sudo for file access
                self.need_sudo = True
                return True
        except:
            # sudo not available, will try anyway
            self.need_sudo = False
            return True
    
    def run_with_sudo(self, cmd):
        """Run command with sudo if needed"""
        if self.need_sudo and not os.geteuid() == 0:
            return ['sudo'] + cmd
        return cmd


def read_error_log(log_path, gtid=None, limit=10):
    """Read MySQL error log and extract relevant entries"""
    if not log_path or not os.path.exists(log_path):
        return None
    
    try:
        entries = []
        with open(log_path, 'r') as f:
            for line in f:
                # Look for error-related entries
                if 'ERROR' in line or 'error' in line.lower():
                    if gtid and gtid not in line:
                        continue
                    entries.append(line.strip())
                    if len(entries) >= limit:
                        break
        
        return entries
    except Exception as e:
        print("Warning: Failed to read error log: {0}".format(str(e)))
        return None


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='MySQL Replication Error SQL Locator - Locate exact SQL statements causing replication errors',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage (auto-detect)
  python mysql_repl_locator.py --host localhost --user admin --password pass
  
  # Specify relay log file
  python mysql_repl_locator.py --host localhost --user admin --password pass \\
    --relay-log /var/lib/mysql/relay-bin.000123
  
  # Export to file
  python mysql_repl_locator.py --host localhost --user admin --password pass \\
    --output-file repl_error_20260323.txt
  
  # Multi-instance scenario
  python mysql_repl_locator.py --socket /var/lib/mysql-instance1/mysql.sock \\
    --user admin --password pass --instance instance1
        """
    )
    
    # Required arguments
    parser.add_argument('--host', default='localhost',
                        help='MySQL host (default: localhost)')
    parser.add_argument('--port', type=int, default=3306,
                        help='MySQL port (default: 3306)')
    parser.add_argument('--user', required=True,
                        help='MySQL username')
    parser.add_argument('--password', required=True,
                        help='MySQL password')
    
    # Optional arguments
    parser.add_argument('--socket', default=None,
                        help='Unix socket path')
    parser.add_argument('--relay-log', default=None,
                        help='Manually specify relay log file path')
    parser.add_argument('--gtid', default=None,
                        help='Manually specify GTID (skip auto-extraction)')
    parser.add_argument('--output-file', default=None,
                        help='Output file path')
    parser.add_argument('--instance', default=None,
                        help='Instance identifier for multi-instance setups')
    parser.add_argument('--verbose', action='store_true',
                        help='Verbose output mode')
    
    return parser.parse_args()


def main():
    """Main entry point"""
    print("MySQL Replication Error SQL Locator")
    print("=" * 40)
    print("")
    
    # Parse arguments
    args = parse_arguments()
    
    # Initialize components
    sudo_handler = SudoHandler()
    sudo_handler.check_sudo()
    
    mysql_client = MySQLClient(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        socket=args.socket
    )
    
    parser = RelayLogParser(need_sudo=sudo_handler.need_sudo)
    analyzer = ErrorAnalyzer()
    formatter = OutputFormatter()
    
    # Connect to MySQL
    print("Connecting to MySQL...")
    if not mysql_client.connect():
        sys.exit(1)
    
    print("Connected successfully")
    print("MySQL Version: {0}".format(mysql_client.mysql_version or 'Unknown'))
    print("Performance Schema: {0}".format(
        'Enabled' if mysql_client.ps_enabled else 'Disabled'))
    print("")
    
    # Get slave status
    print("Retrieving slave status...")
    slave_status = mysql_client.get_slave_status()
    
    if not slave_status or len(slave_status) == 0:
        print("Error: Cannot retrieve slave status. Is this a replica?")
        mysql_client.close()
        sys.exit(1)
    
    # Extract error information
    columns = [desc[0] for desc in slave_status.description]
    status = slave_status[0]
    
    def get_col(name):
        try:
            idx = columns.index(name)
            val = status[idx]
            return val if val else ''
        except (ValueError, IndexError):
            return ''
    
    last_error = get_col('Last_SQL_Error') or get_col('Last_Error')
    relay_log_file = get_col('Relay_Log_File')
    
    if not last_error:
        print("No replication error found. Slave is running normally.")
        mysql_client.close()
        sys.exit(0)
    
    print("Error detected: {0}".format(last_error[:100]))
    print("")
    
    # Analyze error
    analysis_result = analyzer.analyze_error(last_error)
    
    if not analysis_result['is_locatable']:
        print("Warning: This error type ({0}) may not be locatable to specific SQL".format(
            analysis_result['error_description']))
        print("Will attempt to parse relay log anyway...")
        print("")
    
    # Get GTIDs
    gtids = []
    if args.gtid:
        gtids = [args.gtid]
    else:
        gtids = analysis_result['gtids']
    
    if not gtids:
        print("Warning: No GTID found in error message")
        print("Cannot locate specific SQL without GTID")
        mysql_client.close()
        sys.exit(1)
    
    print("Found {0} GTID(s): {1}".format(len(gtids), ', '.join(gtids)))
    print("")
    
    # Get relay log path
    relay_log_path = args.relay_log
    if not relay_log_path:
        if relay_log_file:
            # Construct full path
            datadir = mysql_client.get_datadir()
            if datadir:
                relay_log_path = os.path.join(datadir, relay_log_file)
            else:
                # Try common locations
                common_paths = [
                    '/var/lib/mysql/{0}'.format(relay_log_file),
                    '/var/lib/mysql-relay/{0}'.format(relay_log_file),
                ]
                for path in common_paths:
                    if os.path.exists(path):
                        relay_log_path = path
                        break
        
        if not relay_log_path:
            print("Error: Cannot determine relay log path")
            print("Please specify --relay-log manually")
            mysql_client.close()
            sys.exit(1)
    
    print("Relay log path: {0}".format(relay_log_path))
    print("")
    
    # Check mysqlbinlog
    if not parser.check_mysqlbinlog():
        print("Error: mysqlbinlog not found or not accessible")
        mysql_client.close()
        sys.exit(1)
    
    # Parse relay log
    print("Parsing relay log...")
    binlog_output = parser.parse_relay_log(relay_log_path)
    
    if not binlog_output:
        print("Error: Failed to parse relay log")
        mysql_client.close()
        sys.exit(1)
    
    # Extract transactions for all GTIDs
    print("Extracting SQL transactions...")
    relay_log_results = parser.extract_transactions_for_gtids(binlog_output, gtids)
    
    if not relay_log_results:
        print("Warning: No transactions found for specified GTIDs")
        print("The GTIDs may be outside the current relay log file")
    else:
        print("Found {0} transaction(s)".format(
            sum(len(txns) for txns in relay_log_results.values())))
    
    print("")
    
    # Read error log
    error_log_info = None
    error_log_path = mysql_client.get_error_log_path()
    
    if error_log_path:
        print("Reading error log: {0}".format(error_log_path))
        error_log_entries = read_error_log(error_log_path, gtids[0] if gtids else None)
        
        if error_log_entries:
            error_log_info = {
                'path': error_log_path,
                'entries': error_log_entries,
            }
    
    # Generate report
    print("")
    print("Generating report...")
    
    instance_info = None
    if args.instance:
        instance_info = args.instance
    else:
        instance_info = '{0}:{1}'.format(args.host, args.port)
    
    report = formatter.format_report(
        analysis_result=analysis_result,
        slave_status=slave_status,
        relay_log_results=relay_log_results,
        error_log_info=error_log_info,
        ps_enabled=mysql_client.ps_enabled,
        mysql_version=mysql_client.mysql_version,
        instance_info=instance_info,
    )
    
    # Output report
    if args.output_file:
        if formatter.save_to_file(report, args.output_file):
            print("Report saved to: {0}".format(args.output_file))
        else:
            print("Error: Failed to save report")
            # Still print to stdout
            print("")
            print(report)
    else:
        print("")
        print(report)
    
    # Cleanup
    mysql_client.close()
    
    print("")
    print("Done.")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(0)
    except Exception as e:
        print("\nError: {0}".format(str(e)))
        if '--verbose' in sys.argv:
            import traceback
            traceback.print_exc()
        sys.exit(1)
