#STRUCTURE -> .asc files have this distribution:

#ncols 5201                         W-E / -65º 65.025º / lon
#nrows 4001                         N-S / 65.025º -35º / lat
#xllcorner -65
#yllcorner -35
#cellsize 0.025
#NODATA_value -9999
#->ALL_DATA

#Is important to know that Y(S-N) goes from -35 to 65.025 and X(W-E) goes from -65 to 65.025


import glob
import sys
import json
import os
import boto3
from osgeo import gdal
from osgeo import ogr as ogr
import psycopg2
import threading


#Manually set this True for local testing or False for production on aws
def isLocal():
    return False

#Finds from the file title (as an array splitted by "_") what month and measured element is about
def switch(x):
    return{
        '0':'"Average global irradiance on a horizontal surface"',
        '2a':'"Average global irradiance on a two-axis sun-tracking surface"',
        'opt':'"Average global irradiance on an optimally inclined surface"',
        '01':"1",
        '02':"2",
        '03':"3",
        '04':"4",
        '05':"5",
        '06':"6",
        '07':"7",
        '08':"8",
        '09':"9",
        '10':"10",
        '11':"11",
        '12':"12",
        'year':"0"
    }.get(x,"none")

#Poligonize raster file into GeoJSON features (file with json lines)
def poligonize(file):
    print("Converting file " + file + " to json")
    src_ds = gdal.Open(file)
    if src_ds.RasterCount != 1:
        print("Raster issue - more than one band detected")
        sys.exit(1)
    if src_ds is None:
        print ('Unable to open %s' % file)
        sys.exit(1)
    try:
        srcband = src_ds.GetRasterBand(1)
    except RuntimeError as e:
        print ('Band ( %i ) not found' % srcband)
        print (e)
        sys.exit(1)

    #if everything is OK it starts processing
    dst_layername = file[:-4]
    drv = ogr.GetDriverByName("GeoJSONSeq")
    dst_ds = drv.CreateDataSource(dst_layername + ".json")
    dst_layer = dst_ds.CreateLayer(dst_layername, srs=None)

    #Add the value field to the layer
    precipField = ogr.FieldDefn('value', ogr.OFTInteger)
    dst_layer.CreateField(precipField)

    #Create the GEOJSON
    gdal.Polygonize(srcband, None, dst_layer, 0, [], callback=None)

    print("Converted")

    ######################

    print("Formatting correct fields")

    #Extracts month and measured element from the file name
    data=dst_layername.split("_")
    if(data[0].split("-")[2]=="optang"):
        features='"month":"0","measuredelement":"Optimal inclination angle for an equator-facing plane","unitofmeasure":"degrees"'
    else:
        features='"month":"'+switch(data[2])+'","measuredelement":'+switch(data[1])+',"unitofmeasure":"W/m2"'

    #For every json created from polygonization ->
    with open(""+dst_layername+"_final.json",'w', encoding='utf-8') as output:
        with open(""+dst_layername+".json") as jsonfile:
            #PostGis connection
            if(isLocal()==True):
                connection = psycopg2.connect("dbname=builthubdb user=user password=password")
            else:
                connection = psycopg2.connect("dbname=builthubdb user=user password=password host=localhost")
            cursor = connection.cursor()

            #For each json line ->
            for linea in jsonfile:
                try:
                    jsonobj=json.loads(str(linea))
                except:
                    continue
                if(jsonobj['properties']['value']!=-9999):
                    #Value of irradiance
                    valueStr='"value":'+str(jsonobj['properties']['value'])

                    #Coordinates of the polygon
                    coordinates=jsonobj['geometry']['coordinates'][0]
                    geometryStr=",POLYGON (("
                    for i in coordinates:
                        geometryStr=geometryStr+str(i[0])+" "+str(i[1])+","
                    geometryStr=geometryStr[1:-1]

                    #"NUTS 0" of the polygon (only if it has exactly 1 nuts)
                    nutsStr=""
                    cursor.execute("SELECT nuts_id FROM \"public\".\"builthub_nuts_gis\" WHERE ST_Intersects(ST_GeomFromText('"+geometryStr+"))', 4326), geom) AND levl_code = 0")
                    records = cursor.fetchall()
                    if(sum(map(len,records))==1):
                        nutsStr=nutsStr+'"nuts":"'+records[0][0]+'"'
                        geometryStr='"geometry":"'+geometryStr+'))"'
                        string="{"+nutsStr+","+valueStr+","+geometryStr+","+features+"}"

                        #Write json string into file or variabre
                        output.write(string)
                        output.write('\n')

    print("Formatted")

    if(isLocal()==False):
        s3 = boto3.resource('s3')
        s3.meta.client.upload_file(""+dst_layername+"_final.json", "builthub-draftbox", "dataset030/"+dst_layername+"_final.json")
        os.remove(""+dst_layername+"_final.json")

    print("Saved "+dst_layername+"_final.json")

    return

#*****************MAIN******************#

#Delete the previous .asc in case the previous job didn't finish
filelist = glob.glob(os.path.join(os.getcwd(), "*.asc"))
for fil in filelist:
    os.remove(fil)

#Delete the previous GeoJSON in case the previous job didn't finish
filelist = glob.glob(os.path.join(os.getcwd(), "*.json"))
for fil in filelist:
    os.remove(fil)

#For each one of the .asc files in the same directory ->
if(isLocal()==False):
    s3_client = boto3.client('s3')
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('builthub-rawbox-dev')

    for object in bucket.objects.filter(Prefix='dataset030/'):
        if(object.key!="dataset030/"):
            s3_client.download_file('builthub-rawbox-dev', object.key, object.key.split("/")[1])

p1=0
p2=0
p3=0
for file in glob.glob("*.asc"):
    if(p1==0):
        p1=1
        t1 = threading.Thread(target=poligonize, args=[file])
        t1.start()
        continue
    if(p2==0):
        p2=1
        t2 = threading.Thread(target=poligonize, args=[file])
        t2.start()
        continue
    if(p3==0):
        p3=1
        t3 = threading.Thread(target=poligonize, args=[file])
        t3.start()
    t1.join()
    t2.join()
    t3.join()
    p1=0
    p2=0
    p3=0
if(p1==1):
    t1.join()
    p1=0
if(p2==1):
    t2.join()
    p2=0
if(p3==1):
    t3.join()
    p3=0

#Delete the temporal .asc
filelist = glob.glob(os.path.join(os.getcwd(), "*.asc"))
for fil in filelist:
    os.remove(fil)

#Delete the temporal GeoJSON
filelist = glob.glob(os.path.join(os.getcwd(), "*.json"))
for fil in filelist:
    os.remove(fil)

print('DONE')