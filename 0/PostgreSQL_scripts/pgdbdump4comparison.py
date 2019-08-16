#!/usr/bin/env python3

# -*- coding: ascii -*-

"""
  Python wrapper around `pg_dump` for performing comparable
(diff-friendly) plaintext PostgreSQL database dumps.
"""

#   Standard imports.
#   Import the standard arguments parser.
import argparse
#   Import the standard date, datetime and timedelta system.
from datetime import datetime as dt
#   Import fixed point arithmetic.
from decimal import Decimal, InvalidOperation
#   Import the passwords input library.
from getpass import getpass
#   Import 'inspect' for detailed error tracing.
import inspect
#   Import the standard logging system.
import logging
#   Import 'os' for system calls.
import os
#   Import 'sys' for the 'exit(...)' function.
import sys

#   Non-standard imports.
#   Import the child controlling library.
import pexpect
#   Import the PostgreSQL client library.
import psycopg2


def sort_dx(rows_list, table_name, database_schema_dict):
    """
      Sort a list of rows... by all needed columns.
    :param rows_list: a list.
    :param table_name:
    :param database_schema_dict:
    :returns: a sorted list.
    """
    def row_to_tuple(row, table_name_, database_schema_dict_):
        """
          Return a hashable tuple, for ordering all rows.
        :param row:
        :param table_name_:
        :param database_schema_dict_:
        :return:
        """
        table_description = database_schema_dict_[table_name_]
        logging.debug('row: ' + str(eval('row')))
        #   Escape EOL before splitting by tabulators.
        data = row[:-1].split('\t')
        logging.debug('data: ' + str(eval('data')))
        for i in range(len(data)):
            if table_description[i][1] == 'integer':
                if data[i] != '\\N':
                    #   Null values escaped at this point.
                    data[i] = int(data[i])
            elif table_description[i][1] == 'numeric':
                if data[i] != '\\N':
                    #   Null values escaped at this point.
                    try:
                        data[i] = Decimal(data[i])
                    except InvalidOperation:
                        print('data[i]: ' + str(eval('data[i]')))
                        raise
            #   All remaining types (timestamp without time zone, date,
            # character varying, character varying(length), boolean,
            # binary, etc.) are sort-compatible with the alphabetical
            # string sorting.
        return data

    '''
    def line_to_tuple(line):
        data = line.split()
        return (data[0],int(data[1]),int(data[2]))

    This will turn each line into a tuple which will sort lexicographically.
    Since your strings (the first column) are set up in an easily sorted
    manner, we don't need to worry about them. The second and third columns
    just need to be converted to integers to make them sort properly.

    with open(inputfile) as fin, open(outputfile,'w') as fout:
        non_blank_lines = (line for line in fin if line.strip())
        sorted_lines = sorted(non_blank_lines,key=line_to_tuple)
        fout.writelines(sorted_lines)
    '''

    #   Assume all rows have the same number of cells.
    # elements_no = list[0].split('\t')

    # sorted_list = sorted(list, key=lambda x: int(x.split('\t')[0]))
    sorted_list = sorted(
        rows_list, key=lambda x: row_to_tuple(
            x, table_name, database_schema_dict))
    return sorted_list


def run():
    """ Main function. """
    arguments_parser = argparse.ArgumentParser(
        description='pgdbdump4comparison.py: Python wrapper around '
        '`pg_dump` for performing comparable (diff-friendly) plaintext '
        'PostgreSQL database dumps.')
    stdout_args_group = arguments_parser.add_mutually_exclusive_group(
        required=False)
    stdout_args_group.add_argument(
        '-d', '--debug', action='store_true',
        help='Show debug messages.')
    stdout_args_group.add_argument(
        '-v', '--verbose', action='store_true',
        help='Increased standard output')
    stdout_args_group.add_argument(
        '-q', '--quiet', action='store_true', help='Low standard output')
    stdout_args_group.add_argument(
        '-s', '--silent', action='store_true', help='No standard output')
    password_group = arguments_parser.add_mutually_exclusive_group(
        required=True)
    password_group.add_argument(
        '-P', '--with-this-password', metavar='password_as_argument',
        help='Define the password for performing all operations.')
    password_group.add_argument(
        '-W', '--password', action='store_true',
        help='Provide a password for all operations (it will be '
        'prompted).')
    password_group.add_argument(
        '-w', '--no-password', action='store_true', help='Do not use '
        'passwords (assume any other authorization type).')
    arguments_parser.add_argument(
        '-H', '--host', default='localhost',
        help='PostgreSQL host (defaults to \'localhost\').')
    arguments_parser.add_argument(
        '-u', '--database-user', default='postgres',
        help='PostgreSQL database user (defaults to \'postgres\').')
    arguments_parser.add_argument(
        'database_name', help='Database to be dumped.')
    arguments_parser.add_argument(
        'output_file', help='Dump database to this file.')
    #   Parse arguments.
    arguments = arguments_parser.parse_args()
    #   Process arguments.
    #   Configure the logging system.
    if arguments.quiet:
        log_level = logging.FATAL
    elif arguments.verbose:
        log_level = logging.INFO
    elif arguments.debug:
        log_level = logging.DEBUG
    elif not arguments.silent:
        log_level = logging.ERROR
    else:
        log_level = logging.ERROR
    if not arguments.silent:
        log_format = (
            "%(levelname)s "
            "%(filename)s:%(lineno)d (%(funcName)s) "
            "%(message)s")
        logging.basicConfig(
            stream=sys.stdout, level=log_level, format=log_format)
    #   Set password mode.
    if arguments.with_this_password:
        password = arguments.with_this_password
    elif arguments.password:
        password = getpass()
    else:
        #   Avoid lint warning.
        password = None
    #   Build a `pg_dump` command for the pre-data section.
    dump_pre_data_command = ''
    dump_pre_data_command += "sh -c '"
    dump_pre_data_command += 'pg_dump'
    dump_pre_data_command += ' --section=pre-data'
    dump_pre_data_command += ' -h ' + arguments.host
    dump_pre_data_command += ' -U ' + arguments.database_user
    dump_pre_data_command += ' ' + arguments.database_name
    dump_pre_data_command += ' > ' + arguments.output_file + '_pre-data'
    dump_pre_data_command += "'"
    if arguments.no_password:
        return_status_code = os.system(dump_pre_data_command)
    else:
        #   The child controlling library must be used.
        child = pexpect.spawn(dump_pre_data_command)
        child.expect('Password:', 10)
        child.sendline(password)
        iterations = 0
        while child.isalive():
            try:
                child.expect(pexpect.EOF, 1)
            except pexpect.TIMEOUT:
                pass
            iterations += 1
        if not arguments.quiet and not arguments.silent:
            print(str(iterations) + ' seconds slept waiting for <<<' +
                  dump_pre_data_command + '>>>')
        return_status_code = child.exitstatus
    if return_status_code:
        sys.exit(inspect.getframeinfo(inspect.currentframe()))
    #   Query the table names.
    query = ''
    query += 'SELECT table_name '
    query += 'FROM information_schema.tables '
    query += "WHERE table_schema = 'public'"
    query += 'ORDER BY table_name'
    pg_db_connection = psycopg2.connect(
        host=arguments.host, dbname=arguments.database_name,
        user=arguments.database_user, password=password)
    cursor = pg_db_connection.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    #   The variable rows_list will hold the alphabetical ordering of
    # all tables in scope.
    table_names = []
    for row in rows:
        table_names.append(row[0])
    if arguments.verbose:
        print('table_names: ' + str(eval('table_names')))
    #   The schema for all tables will be necessary too, for sorting
    # their rows.
    schema_dict = {}
    for table_name in table_names:
        query = ''
        query += 'SELECT column_name, data_type, character_maximum_length '
        query += 'FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '
        query += "'" + table_name + "'"
        cursor.execute(query)
        rows = cursor.fetchall()
        columns_list = []
        for row in rows:
            columns_list.append(row)
        schema_dict[table_name] = columns_list
    if arguments.verbose:
        print('schema_dict: ' + str(eval('schema_dict')))
    #   Build a `pg_dump` command for the data section.
    dump_data_command = ''
    dump_data_command += "sh -c '"
    dump_data_command += 'pg_dump'
    dump_data_command += ' --section=data'
    dump_data_command += ' -h ' + arguments.host
    dump_data_command += ' -U ' + arguments.database_user
    dump_data_command += ' ' + arguments.database_name
    dump_data_command += ' > ' + arguments.output_file + '_data'
    dump_data_command += "'"
    if arguments.no_password:
        return_status_code = os.system(dump_data_command)
    else:
        #   The child controlling library must be used.
        child = pexpect.spawn(dump_data_command)
        child.expect('Password:', 10)
        child.sendline(password)
        iterations = 0
        while child.isalive():
            try:
                child.expect(pexpect.EOF, 1)
            except pexpect.TIMEOUT:
                pass
            iterations += 1
        if not arguments.quiet and not arguments.silent:
            print(str(iterations) + ' seconds slept waiting for <<<' +
                  dump_data_command + '>>>')
        return_status_code = child.exitstatus
    if return_status_code:
        sys.exit(inspect.getframeinfo(inspect.currentframe()))
    #   Sort the data dump according to the alphabetical order.
    # While sorting the data dump, sort each table by row id too.
    if not arguments.quiet and not arguments.silent:
        t0 = dt.now()
    else:
        #   Lint warning avoidance.
        t0 = None
    #   Open the data dump file.
    data_dump_file = open(arguments.output_file + '_data')
    sorted_tables_d_d_f = open(arguments.output_file + '_data_', 'w')
    d_d_f_line = data_dump_file.readline()
    while d_d_f_line != '':
        if d_d_f_line[:5] == 'COPY ':
            table_name = d_d_f_line.split(' ')[1]
            sorted_tables_d_d_f.write(d_d_f_line)
            d_d_f_line = data_dump_file.readline()
            #   Create a list for all the rows of the table.
            a_list = []
            while d_d_f_line[:3] != '\.\n':
                a_list.append(d_d_f_line)
                d_d_f_line = data_dump_file.readline()
            #   Sort the lines for the rows of the table.
            a_list = sort_dx(a_list, table_name, schema_dict)
            #   Write those lines.
            for row in a_list:
                sorted_tables_d_d_f.write(row)
            sorted_tables_d_d_f.write('\.\n')
        else:
            sorted_tables_d_d_f.write(d_d_f_line)
        d_d_f_line = data_dump_file.readline()
    if not arguments.quiet and not arguments.silent:
        print(str(dt.now() - t0) + ' time spent sorting table rows...')
    #   Data dump file read ended.
    #   Build a `pg_dump` command for the post-data section.
    dump_post_data_command = ''
    dump_post_data_command += "sh -c '"
    dump_post_data_command += 'pg_dump'
    dump_post_data_command += ' --section=post-data'
    dump_post_data_command += ' -h ' + arguments.host
    dump_post_data_command += ' -U ' + arguments.database_user
    dump_post_data_command += ' ' + arguments.database_name
    dump_post_data_command += ' > ' + arguments.output_file + '_post-data'
    dump_post_data_command += "'"
    if arguments.no_password:
        return_status_code = os.system(dump_post_data_command)
    else:
        #   The child controlling library must be used.
        child = pexpect.spawn(dump_post_data_command)
        child.expect('Password:', 10)
        child.sendline(password)
        iterations = 0
        while child.isalive():
            try:
                child.expect(pexpect.EOF, 1)
            except pexpect.TIMEOUT:
                pass
            iterations += 1
        if not arguments.quiet and not arguments.silent:
            print(str(iterations) + ' seconds slept waiting for <<<' +
                  dump_post_data_command + '>>>')
        return_status_code = child.exitstatus
    if return_status_code:
        sys.exit(inspect.getframeinfo(inspect.currentframe()))
    #   Else success is returned.


if __name__ == '__main__':
    run()
