#!/usr/bin/env python3.5
import argparse
import csv
import re
from glob import glob
from os import path

BASE_DIR = path.dirname(path.realpath(__file__))

ACQ_LABEL_FILE = path.join(BASE_DIR, path.join('LABEL', 'ACQ.FMT'))
OBJ_TOKENS = ['OBJECT', 'GROUP']
MULTILINE_TOKENS = [('\"', '\"'), ('(', ')')]
TIMESTAMP_COL_NAME = '\"Time stamp\"'
PRESSURE_ESTIMATE_COL_NAME = '\"P_EST\"'
TARGET_LBL_FILE_KEYS = [
    'MSL:LOCAL_MEAN_SOLAR_TIME',
    'LOCAL_TRUE_SOLAR_TIME',
    'SOLAR_LONGITUDE',
    {'ROVER_COORDINATE_SYSTEM_PARMS': [
        'COORDINATE_SYSTEM_INDEX',
        'COORDINATE_SYSTEM_INDEX_NAME',
        'ORIGIN_ROTATION_QUATERNION'
    ]}
]


def parse_structured_file(label_file):
    label_file_info = {}

    with open(label_file, 'r') as f:
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
                    if obj_key in label_file_info:
                        if not isinstance(label_file_info[obj_key], list):
                            first_value = label_file_info[obj_key]
                            label_file_info[obj_key] = []
                            label_file_info[obj_key].append(first_value)

                        label_file_info[obj_key].append(current_obj)
                    else:
                        label_file_info[obj_key] = current_obj

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
                    label_file_info[multiline_key] = multiline_value
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
                        label_file_info[key] = value.strip('\n')

    return label_file_info


def get_timestamp_and_pressure_col_nums(label_file):
    timestamp_col_num, pressure_col_num = None, None

    column_definitions = parse_structured_file(label_file)['COLUMN']

    assert column_definitions and isinstance(column_definitions, list)

    for column_definition in column_definitions:
        assert isinstance(column_definition, dict)

        if column_definition['NAME'] == TIMESTAMP_COL_NAME:
            timestamp_col_num = column_definition['COLUMN_NUMBER']

        if column_definition['NAME'] == PRESSURE_ESTIMATE_COL_NAME:
            pressure_col_num = column_definition['COLUMN_NUMBER']

    assert timestamp_col_num, pressure_col_num

    return timestamp_col_num, pressure_col_num


def filter_and_flatten(lbl_file_info, target_info):
    target_results = {}

    for target in target_info:
        if isinstance(target, dict):
            for key in target.keys():
                # Merge result dicts
                target_results = {**target_results,
                                  **filter_and_flatten(lbl_file_info[key], target[key])}
        else:
            target_results[target] = lbl_file_info[target]

    return target_results


def get_lbl_information(lbl_file, target_info):
    return filter_and_flatten(parse_structured_file(lbl_file), target_info)


def get_sol_from_filepath(filepath):
    expr = re.compile(r'^DATA/SOL_\d{5}_\d{5}/SOL(\d{5})/.*$')
    return expr.search(filepath).group(1)


def compose_custom_rows(results_filepath,
                        data_filepath,
                        timestamp_col_num,
                        pressure_col_num,
                        lbl_info_values):
    with open(data_filepath, 'r') as data_file, open(results_filepath, 'a') as results_file:
        csv_writer = csv.writer(results_file)

        for row in csv.reader(data_file):
            csv_writer.writerow([row[timestamp_col_num], row[pressure_col_num], *lbl_info_values])


def main():
    parser = argparse.ArgumentParser(description='Script for parsing rover data')

    parser.add_argument('-f', '--resultsFilename',
                        default='results.csv',
                        help='Name of the file to which the results will be written')

    args = parser.parse_args()

    timestamp_col_num, pressure_col_num = get_timestamp_and_pressure_col_nums(ACQ_LABEL_FILE)

    sol_to_acq_lbl_info = {}

    for filepath in glob('DATA/SOL_?????_?????/SOL?????/*.LBL'):
        sol_to_acq_lbl_info[get_sol_from_filepath(filepath)] = \
            get_lbl_information(filepath, TARGET_LBL_FILE_KEYS)

    header_is_written = False

    for filepath in glob('DATA/SOL_?????_?????/SOL?????/*.TAB'):
        sol = get_sol_from_filepath(filepath)

        print('Calculating rows for sol {}...'.format(sol))

        lbl_info = sol_to_acq_lbl_info[sol]

        lbl_info_keys, lbl_info_values = [], []

        for key in sorted(lbl_info):
            lbl_info_keys.append(key)
            lbl_info_values.append(lbl_info[key])

        if not header_is_written:
            with open(args.resultsFilename, 'w') as f:
                csv_writer = csv.writer(f)

                csv_writer.writerow([
                    TIMESTAMP_COL_NAME.strip('\"'),
                    PRESSURE_ESTIMATE_COL_NAME.strip('\"'),
                    *lbl_info_keys
                ])

            header_is_written = True

        compose_custom_rows(args.resultsFilename,
                            filepath,
                            int(timestamp_col_num),
                            int(pressure_col_num),
                            lbl_info_values)


if __name__ == '__main__':
    main()
