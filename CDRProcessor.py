'''
The Johns Hopkins University Applied Physics Laboratory
Author: Gregory Schilsson
CDRProcessor: Class to process CDR files.

Arguments
-d - Set the read directory
-t - Indicate whether to trim empty fields
'''

import os
import csv
import json
import datetime
import argparse


class CDRProcessor(object):

    KEY_FILES = 'files'
    KEY_DATA = 'data'
    KEY_SCHEMAS = 'schemas'

    def __init__(self, dir_path: str, trim_empty_fields: bool = False):
        self.__dir_path = dir_path
        self.__trim_empty_fields = trim_empty_fields
        self.__data_schema = {}
        self.__data_list = []
        self.__file_list = []
        self.__timestamp = ''

    def __repr__(self):
        return ('CDRProcessor looking at {dir_path}. Contains '
                '{data_schema_count} schemas and {data_list_count} items.'
                .format(
                    dir_path=self.__dir_path,
                    data_schema_count=len(self.__data_schema),
                    data_list_count=len(self.__data_list)
                ))

    def __get_file_list(self) -> list:
        file_list = list(filter(lambda file_name:
                                file_name.startswith(('cdr', 'cmr')) and
                                os.path.splitext(file_name)[1] == '',
                                os.listdir(path=self.__dir_path)))
        return file_list

    def __convert_values(self, in_dict: dict, current_schema: dict) -> dict:
        '''
        Converts integer values from str to int
        '''
        out_dict = {}
        for item in in_dict.items():
            key, value = item[0], item[1]
            value_type = current_schema[key]
            if self.__trim_empty_fields and value in [0, '0', '']:
                continue
            if 'INTEGER' in value_type:
                try:
                    out_dict[key] = int(value)
                except ValueError:
                    if value == '':
                        out_dict[key] = None
            else:
                out_dict[key] = value
        return out_dict

    def __get_record_type(self, file_name: str) -> int:
        '''
        Determines the record type by the filename
        '''
        record_type = 0
        if 'cdr' in file_name.lower():
            record_type = 1
        elif 'cmr' in file_name.lower():
            record_type = 2
        else:
            pass
        return record_type

    def get_timestamp(self, re_init: bool = False) -> str:
        '''
        Returns a timestamp for use with output file naming.
        '''

        utcnow = datetime.datetime.now(datetime.timezone.utc)
        if re_init or not self.__timestamp:
            self.__timestamp = \
                "{YYYY}{MM:02d}{DD:02d}_{HH:02d}{mm:02d}{ss:02d}" \
                .format(
                    YYYY=utcnow.year,
                    MM=utcnow.month,
                    DD=utcnow.day,
                    HH=utcnow.hour,
                    mm=utcnow.minute,
                    ss=utcnow.second
                )
        return self.__timestamp

    def process_directory(self) -> None:
        '''
        Tries to process the files in specified directory into Python objects
        '''
        # Reset the contents of the collections before each run
        self.__data_schema.clear()
        self.__data_list.clear()
        file_list = self.__get_file_list()

        for file_name in file_list:
            record_type = self.__get_record_type(file_name)
            abs_file_path = os.path.join(self.__dir_path, file_name)
            with open(abs_file_path, 'r') as in_file:
                csv_reader = csv.reader(in_file)
                header_name_list = next(csv_reader)
                header_type_list = next(csv_reader)
                # Setup the schema for each file
                file_schema = dict(zip(header_name_list, header_type_list))
                if record_type not in self.__data_schema.keys():
                    self.__data_schema[record_type] = file_schema
                try:
                    for row in csv_reader:
                        data_item = dict(zip(file_schema.keys(), row))
                        data_item = self.__convert_values(
                            data_item, file_schema)
                        self.__data_list.append(data_item)
                except UnicodeDecodeError as ud_err:
                    print('{line_num}: {ud_err}'
                          .format(line_num=reader.line_num, ud_err=ud_err))
            self.__file_list.append(file_name)

    def get_data(self) -> dict:
        '''
        Returns data processed from files
        '''
        if len(self.__data_list) == 0:
            raise Exception('No data. Was process_directory() run?')
        # Put together the dictionary for json output
        # data
        data_dict = {}
        if CDRProcessor.KEY_DATA not in data_dict.keys():
            data_dict[CDRProcessor.KEY_DATA] = self.__data_list
        # schemas
        if CDRProcessor.KEY_SCHEMAS not in data_dict.keys():
            data_dict[CDRProcessor.KEY_SCHEMAS] = {}
        for schema_key in self.__data_schema.keys():
            if schema_key not in data_dict[CDRProcessor.KEY_SCHEMAS].keys():
                data_dict[CDRProcessor.KEY_SCHEMAS][schema_key] = \
                    self.__data_schema[schema_key]
        return data_dict

    def write_json_data(self) -> None:
        '''
        Writes the complete data to a json file. Always overwrites
        '''
        # write the data json file
        timestamp = self.get_timestamp()
        data_file_name = 'data-{ts}.json'.format(ts=timestamp)
        if self.__trim_empty_fields:
            data_file_name = 'data-{ts}-trimmed.json'.format(ts=timestamp)
        json_out_file_path = os.path.join(self.__dir_path, data_file_name)
        data_list = self.__data_list
        if data_list:
            with open(json_out_file_path, 'w') as data_out_file:
                json.dump(data_list, data_out_file)

    def write_json_schemas(self) -> None:
        '''
        Writes the schemas to a json file. Always overwrites
        '''
        # write the schema json file
        timestamp = self.get_timestamp()
        schema_file_name = 'schemas-{ts}.json'.format(ts=timestamp)
        if self.__trim_empty_fields:
            schema_file_name = 'schemas-{ts}-trimmed.json'.format(ts=timestamp)
        schema_out_file_path = os.path.join(self.__dir_path, schema_file_name)
        schema_list = self.__data_schema
        if schema_list:
            with open(schema_out_file_path, 'w') as schema_out_file:
                json.dump(schema_list, schema_out_file)

    def write_file_names(self) -> None:
        '''
        Writes all files processed with this timestamp
        '''
        if self.__file_list:
            timestamp = self.get_timestamp()
            list_file_name = 'files-{ts}.log'.format(ts=timestamp)
            out_file_path = os.path.join(self.__dir_path, list_file_name)
            with open(out_file_path, 'w') as files_out_file:
                for file_name in self.__file_list:
                    print(file_name, file=files_out_file)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', action='store', dest='dir_path',
                        type=str, required=False, default='')
    parser.add_argument('-t', action='store', dest='trim_empty_fields',
                        type=bool, required=False, default=False)
    in_args = parser.parse_args()
    dir_path, trim_empty_fields = in_args.dir_path, in_args.trim_empty_fields
    if dir_path:
        processor = \
            CDRProcessor(dir_path=dir_path,
                         trim_empty_fields=trim_empty_fields)
        processor.process_directory()
        processor.write_json_data()
        processor.write_json_schemas()
        processor.write_file_names()
    else:
        current_dir = os.getcwd()
        processor = CDRProcessor(dir_path=current_dir)
        print(processor)
