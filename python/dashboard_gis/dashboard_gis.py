import psycopg2
from SPARQLWrapper import SPARQLWrapper2


#Conexion Graph
sparql = SPARQLWrapper2("http://localhost:7200/repositories/BuiltHub")


#Query
sparql.setQuery("""
PREFIX cbhsv: <http://data.builthub.eu/ontology/cbhsv#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX ns72: <http://data.europa.eu/nuts/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
SELECT DISTINCT ?id ?nutsID ?measuredElement ?msrValue ?msrUnit ?location_geom
WHERE {
        ?s a cbhsv:Dataset030 ;
        dc:identifier ?id ;
        cbhsv:hasNUTS/ns72:level '0.0'^^xsd:decimal ;
        dcat:spatial/geo:hasGeometry/geo:asWKT ?location_geom ;
        cbhsv:hasNUTS/dc:identifier ?nutsID ;
        cbhsv:measuredElement ?measuredElement ;
        cbhsv:measurementValue ?msrValue ;
        cbhsv:measurementUnit ?msrUnit .
}
    """)


#Conexion Postgres
connection = psycopg2.connect("dbname=builthubdb user=user password=password host=localhost")
cursor = connection.cursor()


#DELETE Data
cursor.execute("DELETE FROM \"public\".\"dashboard_gis\"")
connection.commit()


#INSERT Data
for result in sparql.query().bindings:
    try:
        cursor.execute("INSERT INTO \"public\".\"dashboard_gis\" (\"nuts_code\", \"measuredelement\", \"msrvalue\", \"msrunit\", \"location_geom\") VALUES ('"+result["nutsID"].value+"','"+result["measuredElement"].value+"',"+result["msrValue"].value+",'"+result["msrUnit"].value+"','"+result["location_geom"].value+"')")
        connection.commit()
    except:
        print("Error with data: "+result)
cursor.close()
connection.close()

print("DONE")