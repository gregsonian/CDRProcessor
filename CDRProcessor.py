'''
Gregory Schilsson
CDRProcessor

Class to process CDR files.
'''

import os
import csv
import json


class CDRProcessor(object):

    KEY_DATA = 'data'
    KEY_SCHEMAS = 'schemas'

    def __init__(self, dir_path: str):
        self.__dir_path = dir_path
        self.__data_schema = {}
        self.__data_list = []

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
            if value in [0, '0', '']:
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
            abs_file_path = os.path.join(current_path, file_name)
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

    def write_json_file(self) -> None:
        '''
        Writes the complete data to a json file. Always overwrites
        '''
        # write the json file
        data_dict = self.get_data()
        json_out_file_path = os.path.join(self.__dir_path, 'out.json')
        with open(json_out_file_path, 'w') as out_file:
            json.dump(data_dict, out_file)


if __name__ == '__main__':
    current_path = os.getcwd()
    processor = CDRProcessor(current_path)
    processor.process_directory()
    processor.write_json_file()
