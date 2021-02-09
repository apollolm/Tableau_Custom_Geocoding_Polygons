# -----------------------------------------------------------------------------
#
# This file is the copyrighted property of Tableau Software and is protected
# by registered patents and other applicable U.S. and international laws and
# regulations.
#
# You may adapt this file and modify it to fit into your context and use it
# as a template to start your own projects.
#
# -----------------------------------------------------------------------------

""" Tool to adjust vertex order of polygons in a hyper file

This script enables a customer to adjust the vertex order of all polygons in a hyper file
It provides two commands, list and run
 - List: enumerates all tables specifying which columns are of 'GEOGRAPHY' type
 - Run: adjusts the vertex order of all polygons in a .hyper file, writing the output to a new .hyper file
   Run has two modes: auto and invert:
    - auto mode automatically adjusts the vertex order according to the interior-left definition of
      polygons assuming the data comes from a data source that uses a flat-earth topology
    - invert mode inverts the vertex order for all polygons
   All other (non-geography) columns are just copied as is to the output file
   Tables without geography columns are also copied to the output file
"""

import argparse
import shutil
import subprocess
import sys
import csv

from enum import Enum
from pathlib import Path

from tableauhyperapi import HyperProcess, Telemetry, Connection, SqlType, TableName, Name, escape_string_literal

#Set this.  Otherwise, the default size limit is too small and will exit for modest sized polygon WKT.
csv.field_size_limit(100000000)

class ListTables:
    """ Command to list tables with spatial columns in a .hyper file"""

    Description = "Lists all tables in a .hyper file and shows columns of type GEOGRAPHY"
    """ Description of the command """

    def define_args(self, arg_parser):
        """ Adds arguments for the command
        :param arg_parser: The argparse.ArgumentParser to add arguments to
        """
        arg_parser.add_argument("-i", "--input_file", type=Path, metavar="<input.hyper>",
                                required=True, help="Input .hyper file")

    def run(self, args):
        """ Runs the command
        :param args: Arguments from argparse.Namespace
        """
        input_file = Path(args.input_file)
        print("Listing tables with spatial columns")

        # Starts the Hyper Process with telemetry enabled to send data to Tableau.
        # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
        with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            # Connects to existing Hyper file
            with Connection(endpoint=hyper.endpoint,
                            database=input_file) as connection:
                catalog = connection.catalog
                # Iterates over all schemas in the input file
                for schema_name in catalog.get_schema_names():
                    # Iterates over all tables in the current schema
                    for table in catalog.get_table_names(schema=schema_name):
                        table_definition = catalog.get_table_definition(name=table)
                        rows_in_table = connection.execute_scalar_query(query=f"SELECT COUNT(*) FROM {table}")
                        spatial_columns = [c.name for c in table_definition.columns if c.type == SqlType.geography()]
                        if spatial_columns:
                            print(f"Table {table} with {rows_in_table} rows has"
                                  f" {len(spatial_columns)} spatial columns: {spatial_columns}")
                        else:
                            print(f"Table {table} with {rows_in_table} rows has no spatial columns")


class AdjustVertexOrderMode(Enum):
    """ Modes for adjusting vertex order """
    AUTO = "auto"
    INVERT = "invert"


class CsvQueryClass:
    """ Opens CSV and has a method to query out a row of data based on an input """

    def __init__(self):
        self.fieldnames = []
        self.rows = []

    def open_csv(self, path):
        #Open a specified .csv, and read the rows. Later we'll look thru the rows and use the WKT we find to insert into the local_data table for a given table name
        # Using path to current file, create a path that locates CSV file packaged with these examples.
        #path_to_csv = str(Path(__file__).parent / "data" / "municipalities.csv")
        path_to_csv = "municipalities.csv"

        with open(path_to_csv, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            self.fieldnames = reader.fieldnames
            self.rows = [row for row in reader]

    def get_wkt_by_id(self, id):
        #Given a unique id, read thru the rows and pull out the 'WKT' value.
        #The CSV must have a field called WKT
        if len(self.rows) < 1:
            return ''

        for row in self.rows:
            if(id == row['NAME']):
                return row['WKT']


class AppendWKTColumns:
    """ Command to adjust vertex order of all polygons in a .hyper file """

    Description = "Copies tables from a .hyper file to a new file while adding WKT polygon columns to certain tables"
    """ Description of the command """

    def define_args(self, arg_parser):
        """ Adds arguments for the command
        :param arg_parser: The argparse.ArgumentParser to add arguments to
        """
        arg_parser.add_argument("-i", "--input_file", type=Path, metavar="<input.hyper>",
                                required=True, help="Input .hyper file")
        arg_parser.add_argument("-o", "--output_file", type=Path, metavar="<output.hyper>",
                                required=True, help="Output .hyper file")
        arg_parser.add_argument("-w", "--wkt_path", type=Path, metavar="<wkt.csv>",
                                required=True, help="Path to a .csv file containing a WKT column and a unique ID.")
        # arg_parser.add_argument("-id", "--unique_id",
        #                         required=True, help="Column name containing unique ID that will tie the ROW in the WKT to the row in the geocoidng DB.")
        # arg_parser.add_argument("-m", "--mode", type=AdjustVertexOrderMode, choices=list(AdjustVertexOrderMode),
        #                         required=True, help="Vertex order adjustment mode: "
        #                         "(auto | invert). Auto: assuming data comes "
        #                         "from a source with a flat - earth topology, "
        #                         "it automatically adjusts the vertex order according "
        #                         "to the interior - left definition of polygons. "
        #                         "Invert: inverts the vertex order for all polygons.")

    def run(self, args):
        """ Runs the command
        :param args: Arguments from argparse.Namespace
        """
        input_file = args.input_file
        output_file = args.output_file
        wkt_file = Path(args.wkt_path)
        # Grab the CSV
        csv_query = CsvQueryClass()
        csv_query.open_csv(wkt_file)

        subprocess.call(["rm", "-rf", str(output_file)])
        shutil.copyfile(input_file, output_file)
        # Starts the Hyper Process with telemetry enabled to send data to Tableau.
        # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
        with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(endpoint=hyper.endpoint,
                            database=output_file) as connection:
                table_name = TableName("public", "LocalDatamunicipalities)
                geo_name = Name('Geometry')
                id_name = Name('ParentID')
                map_code_name = Name('MapCode')
                connection.execute_query(f"ALTER TABLE {table_name} ADD COLUMN {geo_name} TEXT,"
                                         f" ADD COLUMN {map_code_name} INTEGER").close()
                for mrow in csv_query.rows:
                    id = mrow['OBJECTID']
                    wkt = mrow['WKT']
                    with connection.execute_query(f"UPDATE {table_name}" +
                                                  f" SET {geo_name}={escape_string_literal(wkt)}, {map_code_name}=0" +
                                                  f" WHERE {id_name}={id}") as result:
                        print(f"for {id} - {len(wkt)} added, {result.affected_row_count} rows changed")
        print('done')


def main(argv):
    command_map = {}
    command_map['list'] = ListTables()
    command_map['run'] = AppendWKTColumns()

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="commands", help="Available commands", dest="command")
    subparsers.required = True
    for name, command in command_map.items():
        cmd_parser = subparsers.add_parser(name, help=command.Description)
        command.define_args(cmd_parser)

    args = parser.parse_args(argv)
    command = command_map[args.command]
    command.run(args)


if __name__ == "__main__":
    main(sys.argv[1:])