# -*- coding: utf-8 -*-

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, MultiPolygon
import os
import json

def convert_to_ddmmss(value):
    degrees = int(value)
    minutes = int((value - degrees) * 60)
    seconds = round((value - degrees - minutes / 60) * 3600, 3)
    return f"{abs(degrees):02}° {abs(minutes):02}' {abs(seconds):06.3f}\""


def process_excel_file(input_filepath):
    df = pd.read_excel(input_filepath)
    
    
    file_directory = os.path.dirname(input_filepath)
    file_basename = os.path.basename(input_filepath)
    file_name, file_extension = os.path.splitext(file_basename)
    date_str = file_name.split('_')[-1]
    
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['Easting'].astype(float), df['Northing'].astype(float)), crs="EPSG:3826")
    gdf['fid'] = range(1, len(gdf) + 1)
    gdf = gdf[['fid'] + [col for col in gdf.columns if col != 'fid']]
    
    gdf4326 = gdf.to_crs(epsg=4326)
    gdf4326['lon'] = gdf4326.geometry.x
    gdf4326['lat'] = gdf4326.geometry.y

    gdf['lon'] = gdf4326['lon']
    gdf['lat'] = gdf4326['lat']
    
    output_geojson_filepath = os.path.join(file_directory, f"pile_location_as_install_{date_str}.geojson")
    gdf.to_file(output_geojson_filepath, driver='GeoJSON')
    
    with open(output_geojson_filepath, 'r', encoding='utf-8') as f:
        geojson_data = json.load(f)
    geojson_data['name'] = file_basename
    for feature in geojson_data['features']:
        for prop in feature['properties']:
            if prop not in ['fid', 'lon', 'lat']:
                feature['properties'][prop] = str(feature['properties'][prop])
    with open(output_geojson_filepath, 'w', encoding='utf-8') as f:
        json.dump(geojson_data, f, ensure_ascii=False)
    print(f"GeoJSON output file created: {output_geojson_filepath}")
    
    gdf['buffer'] = gdf.geometry.buffer(1.75, resolution=100, cap_style=1)
    gdf['buffer'] = gdf['buffer'].apply(lambda geom: MultiPolygon([geom]) if geom.geom_type == 'Polygon' else geom)
    
    gdf_buffer = gdf.copy()
    gdf_buffer.set_geometry('buffer', inplace=True)
    
    
    gdf_buffer = gdf_buffer.drop(columns='geometry')
    
    output_pinpile_geojson_filepath = os.path.join(file_directory, f"as_installed_pinpile_{date_str}.geojson")
    gdf_buffer.to_file(output_pinpile_geojson_filepath, driver='GeoJSON')
    
    with open(output_pinpile_geojson_filepath, 'r', encoding='utf-8') as f:
        geojson_data = json.load(f)
    geojson_data['name'] = f"as_installed_pinpile_{date_str}.geojson"
    for feature in geojson_data['features']:
        for prop in feature['properties']:
            if prop not in ['fid', 'lon', 'lat']:
                feature['properties'][prop] = str(feature['properties'][prop])
    with open(output_pinpile_geojson_filepath, 'w', encoding='utf-8') as f:
        json.dump(geojson_data, f, ensure_ascii=False)
    print(f"GeoJSON buffer output file created: {output_pinpile_geojson_filepath}")
    
    gdf4326['Lontitude'] = gdf4326['lon'].apply(convert_to_ddmmss)
    gdf4326['Latitude'] = gdf4326['lat'].apply(convert_to_ddmmss)
    
    df_processed = pd.concat([df.drop(columns=['geometry']), gdf4326[['lon', 'lat', 'Lontitude', 'Latitude']]], axis=1)
    
    with pd.ExcelWriter(input_filepath, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        df_processed.to_excel(writer, sheet_name='processed', index=False)
        
        worksheet = writer.sheets['processed']
        
        for column in df_processed:
            max_length = max(df_processed[column].astype(str).map(len).max(), len(column))
            col_idx = df_processed.columns.get_loc(column)
            col_letter = worksheet.cell(row=1, column=col_idx + 1).column_letter
            worksheet.column_dimensions[col_letter].width = max_length + 2

    print(f"Processed data saved to sheet 'processed' in file: {input_filepath}")

def process_csv_file(input_filepath):
    df = pd.read_csv(input_filepath)
    
    file_directory = os.path.dirname(input_filepath)
    file_basename = os.path.basename(input_filepath)
    file_name, file_extension = os.path.splitext(file_basename)
    date_str = file_name.split('_')[-1]
    
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['Easting'].astype(float), df['Northing'].astype(float)), crs="EPSG:3826")
    gdf['fid'] = range(1, len(gdf) + 1)
    gdf = gdf[['fid'] + [col for col in gdf.columns if col != 'fid']]
    
    gdf4326 = gdf.to_crs(epsg=4326)
    gdf4326['lon'] = gdf4326.geometry.x
    gdf4326['lat'] = gdf4326.geometry.y
    
    gdf['lon'] = gdf4326['lon']
    gdf['lat'] = gdf4326['lat']
    
    output_geojson_filepath = os.path.join(file_directory, f"pinpile_center_{date_str}.geojson")
    gdf.to_file(output_geojson_filepath, driver='GeoJSON')
    
    with open(output_geojson_filepath, 'r', encoding='utf-8') as f:
        geojson_data = json.load(f)
    geojson_data['name'] = file_basename
    for feature in geojson_data['features']:
        for prop in feature['properties']:
            if prop not in ['fid', 'lon', 'lat']:
                feature['properties'][prop] = str(feature['properties'][prop])
    with open(output_geojson_filepath, 'w', encoding='utf-8') as f:
        json.dump(geojson_data, f, ensure_ascii=False)
    print(f"GeoJSON output file created: {output_geojson_filepath}")
    
    gdf4326['Lontitude'] = gdf4326['lon'].apply(convert_to_ddmmss)
    gdf4326['Latitude'] = gdf4326['lat'].apply(convert_to_ddmmss)
    
    df_processed = pd.concat([df.drop(columns=['geometry', 'ins_date']), gdf4326[['lon', 'lat', 'Lontitude', 'Latitude']]], axis=1)
    
    output_excel_filepath = os.path.join(file_directory, f"pinpile_center_{date_str}.xlsx")
    with pd.ExcelWriter(output_excel_filepath, engine='openpyxl') as writer:
        df_processed.to_excel(writer, index=False)
        
        worksheet = writer.sheets['Sheet1']
        
        for column in df_processed:
            max_length = max(df_processed[column].astype(str).map(len).max(), len(column))
            col_idx = df_processed.columns.get_loc(column)
            col_letter = worksheet.cell(row=1, column=col_idx + 1).column_letter
            worksheet.column_dimensions[col_letter].width = max_length + 2

    print(f"Processed data saved to file: {output_excel_filepath}")

def main():
    try:
        current_directory = os.getcwd()
        
        excel_files = [f for f in os.listdir(current_directory) if f.startswith('pile_location_as_install_') and f.endswith('.xlsx')]
        csv_files = [f for f in os.listdir(current_directory) if f.startswith('pinpile_center_') and f.endswith('.csv')]
        
        if excel_files:
            input_filepath = os.path.join(current_directory, excel_files[0])
            process_excel_file(input_filepath)
        
        if csv_files:
            input_filepath = os.path.join(current_directory, csv_files[0])
            process_csv_file(input_filepath)
    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
