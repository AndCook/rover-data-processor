#!/usr/bin/env python3.5
import argparse
import csv
import re
from glob import glob
from os import path

# Core script information
BASE_DIR = path.dirname(path.realpath(__file__))

# Project-specific information
TARGET_FMT_FILE = path.join(BASE_DIR, path.join('LABEL', 'MODRDR6.FMT'))
OBJ_TOKENS = ['OBJECT', 'GROUP']
MULTILINE_TOKENS = [('\"', '\"'), ('(', ')')]
COL_DATA_TYPE_TO_PYTHON_TYPE = {
    'ASCII_REAL': float,
    'ASCII_INTEGER': int
}
COL_TUPLE_NUM_INDEX, COL_TUPLE_NAME_INDEX, COL_TUPLE_TYPE_INDEX = 0, 1, 2

# Default values which are configurable via program input
DEFAULT_RESULTS_CSV_FILE_NAME = 'results.csv'
DEFAULT_MAX_ROW_COUNT = -1  # Either supply the maximum row count to write out or -1 to get all
DEFAULT_TARGET_COL_NAMES = [  # Either supply target col names to source data from or [] to get all
    '\"TIMESTAMP\"',
    '\"PRESSURE\"',
    '\"BOOM1_LOCAL_AIR_TEMP\"'
]

# This variable is designed to be configurable
TARGET_LBL_INFO = []


def parse_structured_file(structured_file):
    """
    Parse the files in this PDS project that conform to a particular structure in the mars rover
    data set to which this project relates.

    In particular, the file types in the project that conform to the format are the .FMT and .LBL
    files.

    :param structured_file: Filepath to a file which will be parsed
    :return:                Dict object storing the key -> value mappings represented in the
                            provided 'structured_file'
    """
    structured_file_info = {}

    with open(structured_file, 'r') as f:
        building_obj = False
        obj_token = ''
        obj_key = ''
        current_obj = {}

        in_multiline = False
        multiline_end_token = ''
        multiline_key = ''
        multiline_value = ''

        for line in f:
            clean_line = line.strip()

            if building_obj:
                if in_multiline:
                    if clean_line and multiline_end_token == clean_line.strip('\n')[-1]:
                        in_multiline = False
                        multiline_value += clean_line
                        current_obj[multiline_key] = multiline_value
                    else:
                        multiline_value += clean_line
                elif clean_line.startswith('END_{}'.format(obj_token)):
                    if obj_key in structured_file_info:
                        if not isinstance(structured_file_info[obj_key], list):
                            first_value = structured_file_info[obj_key]
                            structured_file_info[obj_key] = []
                            structured_file_info[obj_key].append(first_value)

                        structured_file_info[obj_key].append(current_obj)
                    else:
                        structured_file_info[obj_key] = current_obj

                    building_obj = False
                    obj_token = ''
                    current_obj = {}
                else:
                    parsed_line = clean_line.split(' = ')
                    key = parsed_line[0].strip()
                    value = parsed_line[1].strip()

                    for begin_token, end_token in MULTILINE_TOKENS:
                        if value[0] == begin_token and (value[-1] != end_token or len(value) == 1):
                            in_multiline = True
                            multiline_end_token = end_token
                            multiline_key = key
                            multiline_value = value
                            break

                    if not in_multiline:
                        current_obj[key] = value.strip('\n')
            elif in_multiline:
                if clean_line and multiline_end_token == clean_line.strip('\n')[-1]:
                    in_multiline = False
                    multiline_value += clean_line
                    structured_file_info[multiline_key] = multiline_value
                else:
                    multiline_value += clean_line
            elif ' = ' in clean_line:
                parsed_line = clean_line.split(' = ')
                key = parsed_line[0].strip()
                value = parsed_line[1].strip()

                if key in OBJ_TOKENS:
                    building_obj = True
                    obj_token = key
                    obj_key = value
                else:
                    for begin_token, end_token in MULTILINE_TOKENS:
                        if value[0] == begin_token and (value[-1] != end_token or len(value) == 1):
                            in_multiline = True
                            multiline_end_token = end_token
                            multiline_key = key
                            multiline_value = value

                            break

                    if not in_multiline:
                        structured_file_info[key] = value.strip('\n')

    return structured_file_info


def get_col_info(fmt_file, target_col_names):
    """
    Returns the names and numbers of the columns which can be found in the data files of this
    project. Information is sourced from the provided 'fmt_file' and results will be filtered to
    include only 'target_col_names', if provided.

    :param fmt_file:         Structured .FMT file
    :param target_col_names: List of the names of the columns for which we want the corresponding
                             column, or an empty list
    :return:                 A list of tuples containing the target column numbers, names, and
                             python data types which were extracted according to the target
                             information
    """
    col_info_tuple_list = []

    column_definitions = parse_structured_file(fmt_file)['COLUMN']

    assert column_definitions and isinstance(column_definitions, list)

    for column_definition in column_definitions:
        assert isinstance(column_definition, dict)

        if target_col_names:
            for target_col_name in target_col_names:
                if column_definition['NAME'] == target_col_name:
                    col_info_tuple_list.append((
                        int(column_definition['COLUMN_NUMBER']),
                        column_definition['NAME'].strip('\"'),
                        COL_DATA_TYPE_TO_PYTHON_TYPE[column_definition['DATA_TYPE']]
                    ))
        else:
            col_info_tuple_list.append((
                int(column_definition['COLUMN_NUMBER']),
                column_definition['NAME'].strip('\"'),
                COL_DATA_TYPE_TO_PYTHON_TYPE[column_definition['DATA_TYPE']]
            ))

    assert not target_col_names or (len(target_col_names) == len(col_info_tuple_list)), \
        'Not all target col names were found in the supplied label file'

    return col_info_tuple_list


def get_lbl_info(lbl_file_info, target_info):
    """
    Filter the information provided in 'lbl_file_info' to include only data fields represented by
    'target_info', and flatten the results so we are left with a single level of key -> value
    mappings

    :param lbl_file_info: Information that has been parsed from a structured .LBL file
    :param target_info:   A data structure representing the target keys, or nested sub-keys, which
                          should be included in the results
    :return:              A flattened view of the data as key -> value mappings, filtered according
                          to the provided 'target_info'
    """
    target_results = {}

    for target in target_info:
        if isinstance(target, dict):
            for key in target.keys():
                # Merge result dicts
                target_results = {**target_results,
                                  **get_lbl_info(lbl_file_info[key], target[key])}
        else:
            target_results[target] = lbl_file_info[target]

    return target_results


def get_sol_from_filepath(filepath):
    """
    Extract information about the Sol during which data was collected, given a filepath to a data
    file.

    :param filepath: Path to some data file, assumed to match the format
                     'DATA/SOL_<Start Sol>_<End Sol>/SOL<Sol Number>/<Data File>'
                     where all sols are represented to five digits
    :return:         The specific Sol Number in the filepath
    """
    expr = re.compile(r'^DATA/SOL_\d{5}_\d{5}/SOL(\d{5})/.*$')
    return expr.search(filepath).group(1)


def extract_formatted_col_value(row, col_info_tuple):
    """
    Extracts a particular raw value from a row of data and formats it to be the proper python type

    :param row:            Row of data
    :param col_info_tuple: Tuple containing the number, name, and python type information about the
                           column
    :return:               Formatted value of the column number provided in 'col_info_tuple'
                           extracted from 'row' or None if the cell is equal to "UNK"
    """
    raw_value = row[col_info_tuple[COL_TUPLE_NUM_INDEX] - 1].strip()

    if raw_value == 'UNK':
        return None

    return col_info_tuple[COL_TUPLE_TYPE_INDEX](raw_value)


def compose_custom_rows(results_filepath,
                        data_filepath,
                        remaining_row_count,
                        col_info_tuple_list,
                        lbl_info_values):
    """
    Writes out data to the file at 'results_filepath' using data from fields corresponding to the
    columns at 'col_nums' and sourced from the file at 'data_filepath' and data that has already
    been sourced from the .LBL file now stored in 'lbl_info_values'

    :param results_filepath:    Filepath to the file in which all of the data will be stored
    :param data_filepath:       Filepath to the .TAB data file
    :param remaining_row_count: Number of rows that can still be written out,
                                i.e. maxRowCount - Number of rows already written out
    :param col_info_tuple_list: A list of tuples containing the numbers, names, and python data
                                types of the columns targeted for writing to the csv file
    :param lbl_info_values:     Data sourced from the .LBL file
    :return:                    The row count remaining after processing the data in 'data_filepath'
                                i.e. remaining_row_count - number of rows in data_filepath
    """
    with open(data_filepath, 'r') as data_file, open(results_filepath, 'a') as results_file:
        csv_writer = csv.writer(results_file)

        for row in csv.reader(data_file):
            if remaining_row_count == 0:
                return remaining_row_count
            elif remaining_row_count > 0:
                remaining_row_count -= 1

            csv_writer.writerow([
                *[extract_formatted_col_value(row, col_info_tuple)
                  for col_info_tuple in col_info_tuple_list],
                *lbl_info_values
            ])

    return remaining_row_count


def main():
    """
    Main method

    :return: None
    """
    parser = argparse.ArgumentParser(description='Mars rover data processor')

    parser.add_argument('-f', '--resultsFileName',
                        default=DEFAULT_RESULTS_CSV_FILE_NAME,
                        help=('Name of the csv file to which the results of parsing the data will '
                              'be written'))
    parser.add_argument('-n', '--maxRowCount',
                        default=DEFAULT_MAX_ROW_COUNT,
                        type=int,
                        help='Maximum number of rows that will be written to the csv file')
    parser.add_argument('-c', '--targetColNames',
                        default=DEFAULT_TARGET_COL_NAMES,
                        nargs='*',
                        help='Name(s) of the columns to include in the resulting csv')

    args = parser.parse_args()

    col_info_tuple_list = get_col_info(TARGET_FMT_FILE, args.targetColNames)

    sol_to_acq_lbl_info = {}

    for filepath in glob('DATA/SOL_?????_?????/SOL?????/*.LBL'):
        sol_to_acq_lbl_info[get_sol_from_filepath(filepath)] = \
            get_lbl_info(parse_structured_file(filepath), TARGET_LBL_INFO)

    header_is_written = False
    remaining_row_count = args.maxRowCount

    for filepath in glob('DATA/SOL_?????_?????/SOL?????/*.TAB'):
        sol = get_sol_from_filepath(filepath)

        print('Calculating rows for sol {}...'.format(sol))

        lbl_info = sol_to_acq_lbl_info[sol]

        lbl_info_keys, lbl_info_values = [], []

        for key in sorted(lbl_info):
            lbl_info_keys.append(key)
            lbl_info_values.append(lbl_info[key])

        if not header_is_written:
            with open(args.resultsFileName, 'w') as f:
                csv_writer = csv.writer(f)

                csv_writer.writerow([
                    *map(lambda col_info_tuple: col_info_tuple[COL_TUPLE_NAME_INDEX],
                         col_info_tuple_list),
                    *lbl_info_keys
                ])

            header_is_written = True

        remaining_row_count = compose_custom_rows(args.resultsFileName,
                                                  filepath,
                                                  remaining_row_count,
                                                  col_info_tuple_list,
                                                  lbl_info_values)

        if remaining_row_count == 0:
            break


if __name__ == '__main__':
    main()
