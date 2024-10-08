import psycopg2
from SPARQLWrapper import SPARQLWrapper2


#Conexion Graph
sparql = SPARQLWrapper2("http://localhost:7200/repositories/BuiltHub")


#Query
sparql.setQuery("""
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX at: <http://publications.europa.eu/ontology/authority/>
SELECT DISTINCT ?locationID ?location ?altlocation
WHERE {
    ?s dcat:spatial/skos:prefLabel ?location ;
        dcat:spatial/at:authority-code ?locationID .
    OPTIONAL{?s dcat:spatial/skos:altLabel ?altlocation .
    FILTER(lang(?altlocation)="en")}
    FILTER(lang(?location)="en")
}
    """)


#Conexion Postgres
connection = psycopg2.connect("dbname=builthubdb user=user password=password host=localhost")
cursor = connection.cursor()


#DELETE Data
cursor.execute("DELETE FROM \"public\".\"locations\"")
connection.commit()


#INSERT Data
for result in sparql.query().bindings:
    if((result["locationID"].value=="DEU" and result["altlocation"].value=="FRG") or (result["locationID"].value=="GBR" and result["altlocation"].value=="United Kingdom of Great Britain and Northern Ireland") or (result["locationID"].value=="KOR" and result["altlocation"].value=="ROK") or (result["locationID"].value=="PRK" and result["altlocation"].value=="DPRK") or (result["locationID"].value=="SAU" and result["altlocation"].value=="KSA") or (result["locationID"].value=="USA" and result["altlocation"].value=="US") or (result["locationID"].value=="USA" and result["altlocation"].value=="USA")):
        continue
    try:
        cursor.execute("INSERT INTO \"public\".\"locations\" (\"location_id\", \"name\", \"altname\") VALUES ('"+result["locationID"].value+"','"+result["location"].value+"','"+result["altlocation"].value+"')")
        connection.commit()
    except:
        try:
            cursor.execute("INSERT INTO \"public\".\"locations\" (\"location_id\", \"name\") VALUES ('"+result["locationID"].value+"','"+result["location"].value+"')")
            connection.commit()
        except:
            print("Error with data: "+result)
cursor.close()
connection.close()

print("DONE")