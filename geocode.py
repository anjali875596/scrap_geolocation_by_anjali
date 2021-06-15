import csv
import os
import sys
import argparse
import pprint
import googlemaps
import json
import codecs
import ast
import pandas as pd
import utm
CONFIG = {'output_txt': 'sample_output.csv',
          'project_txt': 'project.csv',
          'output_reference': 'sample_geocoding_result.csv',
          'geocode_url': "https://maps.googleapis.com/maps/api/geocode/json",
          'key': "AIzaSyB4qLAovGpIF4p6m4zDy5uQqrp_e0D-cqI"}


class Geocode(object):
    def __init__(self, parsed_args):
        self.input_csv = parsed_args.i
        self.result_csv = parsed_args.o
        self.geocode_results = []
        self.final_results = []

    def fetch_gecode_results(self):
    #     with open(self.input_csv, 'rt', encoding='ansi') as csvfile:
    #         addresses = csv.DictReader(csvfile,
    #                                    fieldnames=("STREET", "HOUSE_NUMBER",
    #                                                "ZIPCODE", "CITY", "STATE", "COUNTRY", "CODE"))
    #         next(addresses)
            try:
                dfs=pd.read_csv("sample_geocoding.csv", encoding='latin-1' )
                
            except:
                dfs=pd.read_csv("sample_geocoding.csv" , sep = ';')

            
            for i in range(0,2):
                row=dfs.iloc[i]
            # for row in addresses:
                try:
                    address, result = self.get_geocode_result(row)
                    pprint.pprint(result)
                    if not result:
                        continue

                    api_result = [address, result]
                    self.geocode_results.append(api_result)

                    address_row = self.get_address_row(result, row,
                                                       api_result)
                    if address_row:
                        pprint.pprint(address_row)
                        self.final_results.append(address_row)
                except Exception as e:
                    print("ERROR", row, e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    print("Type: ", exc_type, "Line No.:", exc_tb.tb_lineno)
                    pass

            # save results into files
            with codecs.open('final_results.json', 'w',  encoding='latin-1') as outfile:
                json.dump(self.final_results, outfile, ensure_ascii=False)
            with codecs.open('geocode_results.json', 'w', encoding="latin-1") as outfile:
                json.dump(self.geocode_results, outfile, ensure_ascii=False)

    @staticmethod
    def get_geocode_result(row):
        address = '{} {},{} {}, {}, {}'.format(
            row['STREET'], row['HOUSE_NUMBER'],
            row['ZIPCODE'], row['CITY'], row['STATE'], row['COUNTRY'])
        print(address)

        gmaps = googlemaps.Client(key=CONFIG.get('key'))
        result = gmaps.geocode(address)
        return address, result

    @staticmethod
    def get_address_row(address_result, row, api_result):
        try:
            address_components = address_result[0]['address_components']
            formatted_address = address_result[0]['formatted_address']
            lat = address_result[0]['geometry']['location']['lat']
            lng = address_result[0]['geometry']['location']['lng']
            place_id = address_result[0]['place_id']
            type_ = address_result[0]['types'][0]
            status = "OK"

            # group add long_name by types
            address_types = {}
            for d in address_components:
                for d_type in d['types']:
                    if d_type in address_types:
                        address_types[d_type].append(d['long_name'])
                    else:
                        address_types[d_type] = [d['long_name']]

            # street_number
            street_number = address_types.get(
                'street_number')[0] if address_types.get('street_number') else ''
            # route
            route = address_types.get(
                'route')[0] if address_types.get('route') else ''
            # locality
            locality = address_types.get(
                'locality')[0] if len(address_types.get('locality')) else ''
            # administrative_area_level_2
            administrative_area_level_2 = address_types.get(
                'administrative_area_level_2')[0] if address_types.get('administrative_area_level_2') else ''
            # administrative_area_level_1
            administrative_area_level_1 = address_types.get(
                'administrative_area_level_1')[0] if address_types.get('administrative_area_level_1') else ''
            # country
            country = address_types.get(
                'country')[0] if address_types.get('country') else ''
            # postal_code
            postal_code = address_types.get(
                'postal_code')[0] if address_types.get('postal_code') else ''

            return [row['STREET'], row['HOUSE_NUMBER'], row['ZIPCODE'],
                    row['CITY'], row['STATE'], row['COUNTRY'],
                    api_result, lng, lat, formatted_address,
                    place_id, type_, status, street_number, route, locality,
                    administrative_area_level_2, administrative_area_level_1,
                    country, postal_code, row['CODE']]
        except Exception as e:
            print("ERROR", row, e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print("Type: ", exc_type, "Line No.:", exc_tb.tb_lineno)
            pass

    def save_output_reference(self):
        # save google geocoding result
        with open(CONFIG.get('output_reference'), 'wt') as csvfile:
            fieldnames = ['address', 'geocoding_result']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for geocode_result in self.geocode_results:
                try:
                    writer.writerow({'address': geocode_result[0],
                                     'geocoding_result': geocode_result[1]})
                except Exception as e:
                    print("ERROR", e)
                    pass

    def save_coordinates_output(self):
        # save coordinates
        with open(CONFIG.get('output_txt'), 'wt') as csvfile:
            writer = csv.writer(csvfile, delimiter=' ', quotechar='"')
            # spamwriter.writerow(["lat","lng"])
            for row in self.final_results:
                try:
                    writer.writerow([row[5], row[6]])
                except Exception as e:
                    print("ERROR", e)
                    pass

    def run_gdal_transform(self):
        # run reprojection
        command = 'gdaltransform -s_srs EPSG:4326 -t_srs EPSG:32631 < {} > {}'
        os.system(command.format(CONFIG.get('output_txt'),
                                 CONFIG.get('project_txt')))

        # add xy
        with open(CONFIG.get('project_txt'), 'rt') as csvfile:
            reader = csv.reader(csvfile, delimiter=' ', quotechar='"')
            i = 0
            for row in reader:
                try:
                    self.final_results[i].append(row[0])
                    self.final_results[i].append(row[1])
                except Exception as e:
                    print("ERROR", row, e)
                    pass
                i = i + 1

    def generate_final_results(self):
        """ Run gdal tranformation """
        self.save_output_reference()
        self.save_coordinates_output()
        self.run_gdal_transform()
        p = []
        import json

        # final result
        with open(self.result_csv, 'w') as csvfile:
            fieldnames = ['STREET', 'HOUSE_NUMBER', 'ZIPCODE', 'CITY',
                          'STATE', 'COUNTRY',
                          'LONGITUDE', 'LATITUDE', 'X', 'Y', 'google_geocode',
                          'formatted_address', 'place_id', 'type', 'status',
                          'street_number', 'route', 'locality',
                          'administrative_area_level_2',
                          'administrative_area_level_1',
                          'country', 'postal_code', 'CODE']
            writer = csv.DictWriter(csvfile, delimiter=";",
                                    fieldnames=fieldnames)
            writer.writeheader()
            for row in self.final_results:
                if row:
                    
                    try:
                        # skip unicode encoding exception
                        row = [item.replace(',', '.') if isinstance(
                            item, str) else item for item in row]

                        # convert byte data into string
                        row = [item.decode('ansi') if isinstance(
                            item, bytes) else item for item in row]
                        print(row, len(row))
                        p.append(row)
                        with open('rows.json', 'w') as f:
                            json.dump(p, f, indent=2)
                        # convert address byte data into normal string
                        google_geocode = []
                        for g_data in row[6]:
                            if isinstance(g_data, bytes):
                                g_data = g_data.decode('ansi')
                            google_geocode.append(g_data)
                        
                        writer.writerow({'STREET': row[0], 'HOUSE_NUMBER': row[1],
                                         'ZIPCODE': row[2], 'CITY': row[3],
                                         'google_geocode': google_geocode,
                                         'LONGITUDE': row[5+2], 'LATITUDE': row[6+2],
                                         'formatted_address': row[7+2],
                                         'place_id': row[8+2],
                                         'type': row[9+2], 'status': row[10+2],
                                         'street_number': row[11+2],
                                         'route': row[12+2], 'locality': row[13+2],
                                         'administrative_area_level_2': row[14+2],
                                         'administrative_area_level_1': row[15+2],
                                         'country': row[16+2],
                                         'postal_code': row[17+2],
                                         'CODE': row[18+2],
                                         'X': row[19],
                                         'Y': row[20]})
                                       #  'X': row[7],
                                       #  'Y': row[8]})
                    except Exception as e:
                        print("ERROR", row, e)
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        print("Type: ", exc_type, "Line No.:", exc_tb.tb_lineno)
                        pass


def create_arg_parser():
    """Creates and returns the ArgumentParser object."""
    parser = argparse.ArgumentParser(description='This file generates geocode results for address. \
        Usage: geocode.py -i <inputfile> -o <outputfile>')
    parser.add_argument('-i',
                        help='Path to the input CSV.',
                        default='sample_geocoding.csv')
    parser.add_argument('-o',
                        help='Path to the output CSV.',
                        default='result.csv')
    return parser


if __name__ == "__main__":
    try:
        arg_parser = create_arg_parser()
        parsed_args = arg_parser.parse_args(sys.argv[1:])

        geocode = Geocode(parsed_args)
        geocode.fetch_gecode_results()
        geocode.generate_final_results()

        # remove unnecessary files
        os.remove(CONFIG.get('output_txt'))
        os.remove(CONFIG.get('project_txt'))
    except Exception as e:
        print("ERROR", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print("Type: ", exc_type, "Line No.:", exc_tb.tb_lineno)
