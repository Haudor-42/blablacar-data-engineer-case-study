#!/usr/bin/python
# -*- coding: utf-8 -*-

#  Modules import
import os
import glob
import logging
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class CustomCleanFilesOperator(BaseOperator):
    """
    Custom Operator created to deal with cleaning operations needed for all ETLs.
    It simply deletes file from Cloud Storage
    """

    template_fields = ['date_str']

    @apply_defaults
    def __init__( self, files, dataset, table_name, date_str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.files = files
        self.dataset = dataset
        self.table_name = table_name
        self.date_str = date_str

    def execute(self, context):

        for file in self.files:

            filepattern = '/home/airflow/gcs/data/' + self.dataset + '.' + self.table_name + file + self.date_str + '.*'
            print(filepattern)
            filelist = glob.glob(filepattern)

            for filename in filelist:
                if os.path.exists(filename):
                    os.remove(filename)
                    logging.info('file cleaned: ' + filename)
                else:
                    logging.info("The file does not exist: " + filename)

        return {
            'task_status': 'Cleaning operation: success',
            'files_deleted': str(self.files)
        }
