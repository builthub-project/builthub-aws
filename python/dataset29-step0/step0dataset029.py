#huss -> Specific Humidity (grams of water vapor per kilogram of air (g/kg))
#hurs -> Relative Humidity
#tas -> air temperature
#check further fields here: https://cera-www.dkrz.de/WDCC/ui/cerasearch/q?query=*:*&entry_type_s=Dataset&hierarchy_steps_ss=CXEU11CLC5hiCL&page=0&rows=100&sort=title_sort%20desc

import glob
import netCDF4
import psycopg2
import boto3
import sys
import datetime
import os
import threading

#Manually set this True for local testing or False for production on aws
def isLocal():
    return False


def workWithNc(nc, name, bands, specialVal):
    print("Band "+str(bands))

    if(specialVal=='huss'):
        unitofmeasureStr='"unitofmeasure":"g/kg"'
        measuredelementStr='"measuredelement":"Specific Humidity"'
    elif(specialVal=='hurs'):
        unitofmeasureStr='"unitofmeasure":"%"'
        measuredelementStr='"measuredelement":"Relative Humidity"'
    elif(specialVal=='tas'):
        unitofmeasureStr='"unitofmeasure":"K"'
        measuredelementStr='"measuredelement":"Air Temperature"'

    with open(name+"_-_"+str(bands)+".json",'w', encoding='utf-8') as output:

        initial_date = datetime.datetime.strptime("1949/12/01", r"%Y/%m/%d")
        exact_date = initial_date + datetime.timedelta(days=int(nc.variables["time"][bands]))

        #Establish conecction with postgis database
        if(isLocal()==True):
            connection = psycopg2.connect("dbname=builthubdb user=user password=password")
        else:
            connection = psycopg2.connect("dbname=builthubdb user=user password=password host=localhost")
        cursor = connection.cursor()

        for i in range(nc.variables[specialVal].shape[1]):
            for j in range(nc.variables[specialVal].shape[2]):
                #Query to Postgis
                try:
                    geometrystr="POINT("+str(nc.variables["longitude"][i][j])+" "+str(nc.variables["latitude"][i][j])+")"
                except:
                    try:
                        geometrystr="POINT("+str(nc.variables["lon"][i][j])+" "+str(nc.variables["lat"][i][j])+")"
                    except:
                        print("Error with latitude/longitude/lat/lon variable")
                        sys.exit(1)

                cursor.execute("SELECT nuts_id FROM \"public\".\"builthub_nuts_gis\" WHERE ST_Within(ST_GeomFromText('"+geometrystr+"',4326), geom) ORDER BY levl_code")
                records = cursor.fetchall()

                if(sum(map(len,records))==4):
                    valueStr='"value":'+str(nc.variables[specialVal][bands][i][j])
                    geometryStr='"geometry":"'+geometrystr+'"'
                    dateStr='"date":"'+str(exact_date).split(" ")[0]+'"'
                    nuts0Str='"nuts0":"'+str(records[0][0])+'"'
                    nuts1Str='"nuts1":"'+str(records[1][0])+'"'
                    nuts2Str='"nuts2":"'+str(records[2][0])+'"'
                    nuts3Str='"nuts3":"'+str(records[3][0])+'"'

                    string="{"+valueStr+","+geometryStr+","+dateStr+","+nuts0Str+","+nuts1Str+","+nuts2Str+","+nuts3Str+","+measuredelementStr+","+unitofmeasureStr+"}"

                    #Write json string into file or variabre
                    output.write(string)
                    output.write('\n')

   #Save the .json file
    if(isLocal()==False):
        s3 = boto3.resource('s3')
        s3.meta.client.upload_file(name+"_-_"+str(bands)+".json", "builthub-draftbox", "dataset029/"+name+"_-_"+str(bands)+".json")
        os.remove(name+"_-_"+str(bands)+".json")

    print("Saved "+name+"_-_"+str(bands)+".json")

    return

#*****************MAIN******************#

#For each one of the .nc files in the same directory ->
if(isLocal()==False):
    #Delete the previous .nc in case the previous job didn't finish
    filelist = glob.glob(os.path.join(os.getcwd(), "*.nc"))
    for fil in filelist:
        os.remove(fil)

    #Delete the previous .json in case the previous job didn't finish
    filelist = glob.glob(os.path.join(os.getcwd(), "*.json"))
    for fil in filelist:
        os.remove(fil)

    s3_client = boto3.client('s3')
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('builthub-rawbox-dev')

    for object in bucket.objects.filter(Prefix='dataset029/'):
        if(object.key!="dataset029/"):
            s3_client.download_file('builthub-rawbox-dev', object.key, object.key.split("/")[1])

for file in glob.glob("*.nc"):
    print("Working with "+file)
    name=str(file)[:-3]
    nc = netCDF4.Dataset(file, mode='r')

    #Set the measured unit and the measured element
    specialVal=""
    for var in nc.variables:
        if(var=='huss'):
            specialVal="huss"
        elif(var=='hurs'):
            specialVal="hurs"
        elif(var=='tas'):
            specialVal="tas"

    #Each band
    p1=0
    p2=0
    p3=0
    p4=0
    p5=0
    p6=0
    p7=0
    p8=0
    for bands in range(nc.variables[specialVal].shape[0]):
        if(p1==0):
            p1=1
            t1 = threading.Thread(target=workWithNc, args=[nc, str(name), bands, specialVal])
            t1.start()
            continue
        if(p2==0):
            p2=1
            t2 = threading.Thread(target=workWithNc, args=[nc, str(name), bands, specialVal])
            t2.start()
            continue
        if(p3==0):
            p3=1
            t3 = threading.Thread(target=workWithNc, args=[nc, str(name), bands, specialVal])
            t3.start()
            continue
        if(p4==0):
            p4=1
            t4 = threading.Thread(target=workWithNc, args=[nc, str(name), bands, specialVal])
            t4.start()
            continue
        if(p5==0):
            p5=1
            t5 = threading.Thread(target=workWithNc, args=[nc, str(name), bands, specialVal])
            t5.start()
            continue
        if(p6==0):
            p6=1
            t6 = threading.Thread(target=workWithNc, args=[nc, str(name), bands, specialVal])
            t6.start()
            continue
        if(p7==0):
            p7=1
            t7 = threading.Thread(target=workWithNc, args=[nc, str(name), bands, specialVal])
            t7.start()
            continue
        if(p8==0):
            p8=1
            t8 = threading.Thread(target=workWithNc, args=[nc, str(name), bands, specialVal])
            t8.start()
        t1.join()
        t2.join()
        t3.join()
        t4.join()
        t5.join()
        t6.join()
        t7.join()
        t8.join()
        p1=0
        p2=0
        p3=0
        p4=0
        p5=0
        p6=0
        p7=0
        p8=0
    if(p1==1):
        t1.join()
        p1=0
    if(p2==1):
        t2.join()
        p2=0
    if(p3==1):
        t3.join()
        p3=0
    if(p4==1):
        t4.join()
        p4=0
    if(p5==1):
        t5.join()
        p5=0
    if(p6==1):
        t6.join()
        p6=0
    if(p7==1):
        t7.join()
        p7=0
    if(p8==1):
        t8.join()
        p8=0

#Delete the temporal .nc
filelist = glob.glob(os.path.join(os.getcwd(), "*.nc"))
for fil in filelist:
    os.remove(fil)


print("DONE")