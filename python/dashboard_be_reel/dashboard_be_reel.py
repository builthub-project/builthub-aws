import psycopg2
import pandas as pd
import pandasql as ps
import osmnx as ox



#Conexion Postgres
connection = psycopg2.connect("dbname=builthubdb user=user password=password host=localhost")
cursor = connection.cursor()


#DELETE Data
cursor.execute("DELETE FROM \"public\".\"dashboard_be_reel\"")
connection.commit()
cursor.execute("DELETE FROM \"public\".\"dashboard_be_reel_gis\"")
connection.commit()


#RETRIVE ALL DATA
all_data = pd.read_excel('VEA.xlsx', sheet_name='Data')
df = pd.DataFrame(all_data)


#RETRIVE SECTOR DATA
cursor.execute("SELECT cd_sector, tx_sector_descr_nl, geom_wkt FROM \"public\".\"belgium_gis\" WHERE tx_munty_descr_nl='Harelbeke'")
results=cursor.fetchall()

#INSERT SECTOR Data
for row in results:
    try:
        place_name = row[1]
        place_name=place_name.replace("'", "''")
        cursor.execute("INSERT INTO \"public\".\"dashboard_be_reel_gis\" (\"place_code\", \"place\", \"place_type\", \"geomwkt\") VALUES ('"+row[0]+"','"+row[1]+"','sector','"+row[2]+"')")
        connection.commit()
    except:
        print("ERROR SECTOR: "+place_name+" "+row[1]+" "+row[2])


#RETRIVE DISTRICT DATA
query1="SELECT DISTINCT \"deelgemeente/wijk code & naam\", \"deelgemeente/wijk code\" FROM df WHERE gemeente='Harelbeke'"
districts = ps.sqldf(query1)


#INSERT DISTRICT Data
for index, row in districts.iterrows():
    try:
        place_name = row[0]
        place_name=place_name.replace(row[1], "")
        place_name=place_name.replace("*", "")
        place_name=place_name.replace("_", "")
        gdf = ox.geocode_to_gdf(place_name, which_result=1)

        cursor.execute("INSERT INTO \"public\".\"dashboard_be_reel_gis\" (\"place_code\", \"place\", \"place_type\", \"geomwkt\") VALUES ('"+row[1]+"','"+place_name+"','district',ST_AsEWKT(ST_FlipCoordinates(GeomFromEWKT('"+str(gdf['geometry'][0])+"'))))")
        connection.commit()
    except:
        print("ERROR DISTRICT: "+row[1]+" "+place_name)



print("DONE (1/2)")



#INSERT Data
all_data = pd.read_excel('VEA.xlsx', sheet_name='Data')
for index, row in df.iterrows():
    try:
        if(str(row["tot_aantal_kadaster"])=="nan"):
            row["tot_aantal_kadaster"]=0
        if(str(row["pic_ratio"])=="nan"):
            row["pic_ratio"]=0
        if(str(row["bouwperiod"])=="voor 1900"):
            row["bouwperiod"]="0000-1900"
        if(str(row["bouwperiod"])=="onbekend"):
            row["bouwperiod"]="Unknown"

        cursor.execute("INSERT INTO \"public\".\"dashboard_be_reel\" (\"dashboard_id\", \"accommodations\", \"sector_code\", \"district_code\", \"resident\", \"construction_form\", \"construction_period\", \"change_period\", \"municipality\", \"cluster\", \"post_code\", \"surface\", \"specific_theoretical_energy_consumption_kwhperm2\", \"specific_corrected_energy_consumption_kwhperm2\", \"epc\", \"flags\", \"land_register\", \"pic_ratio\", \"flag_pic_ratio2\", \"flag_extrap_2\", \"flag_extrap_3\", \"flag_pic_ratio3\", \"total_theoretical_energy_consumption_mwhperyear\", \"total_adjusted_energy_consumption_mwhperyear\", \"calibration_ratio\", \"flag_cf_30\", \"flag_cf\", \"total_calibrated_energy_consumption_mwhperyear\", \"adjusted_reduction_potential_mwhperyear\", \"adjusted_reduction_potential_per_number_of_housing_units_mwhperyear\", \"specifically_calibrated_energy_consumption_kwhperm2\", \"theoretical_reduction_potential_mwhperyear\", \"theoretical_reduction_potential_per_number_of_housing_units_mwhperyear\") VALUES ('"+row["key"]+"',"+str(row["aantal_wooneenheid​"])+",'"+row["stat sect code"]+"','"+row["deelgemeente/wijk code"]+"','"+row["bewonerstype"]+"','"+row["bouwvorm"]+"','"+row["bouwperiod"]+"','"+row["wijzigingsjaar"]+"','"+row["gemeente"]+"',"+str(row["cluster"])+",'"+str(row["postcode"])+"',"+str(row["oppervlakte"])+","+str(row["Specifiek theoretisch energieverbruik (kWh/m2)​"])+","+str(row["Specifiek gecorrigeerd energieverbruik (kWh/m2)​"])+","+str(row["Aantal epc"])+",'"+str(row["Flag_extrapolation"])+"',"+str(row["tot_aantal_kadaster"])+","+str(row["pic_ratio"])+",'"+str(row["Flag_pic_ratio2"])+"','"+str(row["Flag_extrap_2"])+"','"+str(row["Flag_extrap_3"])+"','"+str(row["Flag_pic_ratio3"])+"',"+str(row["Totaal theoretisch energieverbruik ​(MWh/jaar)"])+","+str(row["Totaal gecorrigeerd energieverbruik ​(MWh/jaar)"])+",'"+str(row["Calibration Factor"])+"','"+str(row["Flag CF_30"])+"','"+str(row["Flag CF"])+"',"+str(row["Totaal gekalibreerd energieverbruik ​(MWh/jaar)"])+","+str(row["Gecorrigeerd reductiepotentieel (MWh/jaar)"])+","+str(row["Gecorrigeerd reductiepotentieel /aantal wooneenheid (MWh/jaar)"])+","+str(row["Specifiek gekalibreerd energieverbruik (kWh/m2)​"])+","+str(row["Theoretisch reductiepotentieel (MWh/jaar)"])+","+str(row["Theoretisch reductiepotentieel /aantal wooneenheid (MWh/jaar)"])+")")
        connection.commit()
    except:
        print("Error with data: "+row)
cursor.close()
connection.close()


print("DONE (2/2)")