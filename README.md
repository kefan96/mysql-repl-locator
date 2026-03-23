# MySQL Replication Error SQL Locator

A Python tool to locate the exact SQL statement that caused replication errors on MySQL replica servers.

## Features

- **Locate Failed SQL**: When replication fails (e.g., duplicate key errors), find the exact SQL statement that caused the failure
- **GTID-based Analysis**: Extract GTID from error messages and locate corresponding transactions in relay logs
- **Multiple GTID Support**: Handle errors involving multiple GTIDs
- **Performance Schema Integration**: Use performance_schema for enhanced information when available
- **Graceful Degradation**: Works even when performance_schema is disabled
- **Multi-Version Support**: Compatible with MySQL 5.6, 5.7, and 8.0+
- **Python 2.7/3.x Compatible**: Works with both Python 2.7 and Python 3.x
- **Multi-Instance Support**: Handle multiple MySQL replica instances on the same server
- **Automatic Sudo Detection**: Automatically detects and handles sudo requirements

## Requirements

### Python Dependencies

- Python 2.7 or Python 3.x
- `pymysql` or `MySQLdb` package

```bash
# For Python 3
pip3 install pymysql

# For Python 2
pip2 install pymysql
```

### System Requirements

- `mysqlbinlog` utility (included with MySQL installation)
- Read access to MySQL relay log files (may require sudo)
- MySQL user with appropriate privileges:
  - `REPLICATION SLAVE` - Read replication status
  - `PROCESS` - View process information
  - `SELECT` on `performance_schema` - Query performance data (if enabled)

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd mysql-repl-locator

# Make the script executable
chmod +x mysql_repl_locator.py
```

## Usage

### Basic Usage

```bash
python mysql_repl_locator.py --host localhost --user admin --password your_password
```

### Specify Relay Log File

```bash
python mysql_repl_locator.py --host localhost --user admin --password your_password \
  --relay-log /var/lib/mysql/relay-bin.000123
```

### Export to File

```bash
python mysql_repl_locator.py --host localhost --user admin --password your_password \
  --output-file repl_error_20260323.txt
```

### Multi-Instance Setup

```bash
# Using socket
python mysql_repl_locator.py --socket /var/lib/mysql-instance1/mysql.sock \
  --user admin --password your_password --instance instance1

# Using port
python mysql_repl_locator.py --host localhost --port 3307 \
  --user admin --password your_password --instance instance2
```

### Manually Specify GTID

```bash
python mysql_repl_locator.py --host localhost --user admin --password your_password \
  --gtid "3c7a8b9d-1234-5678-9abc-def012345678:12345"
```

### Verbose Mode

```bash
python mysql_repl_locator.py --host localhost --user admin --password your_password \
  --verbose
```

## Command Line Options

| Option | Description | Required |
|--------|-------------|----------|
| `--host` | MySQL host (default: localhost) | No |
| `--port` | MySQL port (default: 3306) | No |
| `--user` | MySQL username | Yes |
| `--password` | MySQL password | Yes |
| `--socket` | Unix socket path | No |
| `--relay-log` | Manually specify relay log file path | No |
| `--gtid` | Manually specify GTID | No |
| `--output-file` | Output file path | No |
| `--instance` | Instance identifier | No |
| `--verbose` | Verbose output mode | No |

## How It Works

1. **Error Collection**: Executes `SHOW SLAVE STATUS` to get replication status and extract error information
2. **GTID Extraction**: Parses error messages to extract GTID(s)
3. **Relay Log Location**: Determines the relay log file path (automatically or manually)
4. **Relay Log Parsing**: Uses `mysqlbinlog` to parse relay log and locate transactions for the extracted GTIDs
5. **Enhanced Information**: Queries `performance_schema` if available for additional context
6. **Error Log Analysis**: Reads MySQL error log for related entries
7. **Report Generation**: Generates a comprehensive report with all findings

## Supported Error Types

The tool can locate the following error types:

- **1062**: Duplicate entry (Primary/Unique key conflict)
- **1452**: Foreign key constraint fails
- **1146**: Table doesn't exist
- **1054**: Unknown column
- **1064**: SQL syntax error
- **1364**: Field doesn't have a default value
- **1048**: Column cannot be null

## Output Example

```
============================================================
MySQL Replication Error SQL Locator Report
============================================================
Generated: 2026-03-23 15:30:45
Instance: localhost:3306

[Error Overview]
Error Code: 1062
Error Type: Duplicate entry (Primary/Unique key conflict)
Error Message: Could not execute Write_rows event on table db.table...
Relay Log File: relay-bin.000123
Slave IO Running: Yes
Slave SQL Running: No

[Performance Schema Status]
Status: Enabled (Enhanced Mode)
MySQL Version: 5.7

[Located SQL Transactions]

----------------------------------------
GTID: 3c7a8b9d-1234-5678-9abc-def012345678:12345
----------------------------------------
# at 123456
#230320 14:30:25 server id 1 end_log_pos 123789
BEGIN
/*!*/;
# at 123500
#230320 14:30:25 server id 1 end_log_pos 123789
UPDATE `db`.`table` SET `col1`='value1', `col2`='value2' WHERE `id`=123
# at 123789
COMMIT/*!*/;

[Suggested Actions]
1. Review the located SQL statements above
2. Check for duplicate key conflicts on the replica
3. Consider using pt-table-checksum to verify data consistency
4. You may skip this GTID or manually fix the data

============================================================
```

## Troubleshooting

### Permission Denied

If you get "Permission denied" errors when accessing relay log files:

```bash
# Run with sudo
sudo python mysql_repl_locator.py --host localhost --user admin --password pass

# Or add your user to the mysql group
sudo usermod -a -G mysql $USER
```

### mysqlbinlog Not Found

Ensure `mysqlbinlog` is in your PATH:

```bash
# Check if available
which mysqlbinlog

# Add to PATH if needed
export PATH=$PATH:/usr/local/mysql/bin
```

### No GTID Found

If no GTID is found in the error message:

1. Ensure GTID-based replication is enabled on the master
2. Manually specify the GTID using `--gtid` option
3. Check MySQL error log for GTID information

### Performance Schema Disabled

If performance_schema is disabled, the tool will work in basic mode:

- Only relay log parsing will be used
- Some enhanced information will not be available
- The tool will still locate the SQL statements

## License

This tool is provided as-is for MySQL replication troubleshooting.

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## Author

Created by kefan
