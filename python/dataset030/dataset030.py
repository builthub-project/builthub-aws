import psycopg2
from SPARQLWrapper import SPARQLWrapper2


#Conexion Graph
sparql = SPARQLWrapper2("http://localhost:7200/repositories/BuiltHub")


#Query
sparql.setQuery("""
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX cbhsv: <http://data.builthub.eu/ontology/cbhsv#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX ns72: <http://data.europa.eu/nuts/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX ns71: <http://purl.org/dc/terms/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT DISTINCT ?id ?nutsID ('30'^^xsd:integer as ?datasetID) ?freq ?startDate ?endDate ?measuredElement ?msrValue ?msrUnit ?location_geom
WHERE {
    ?s a cbhsv:Dataset030 ;
        dc:identifier ?id ;
        cbhsv:hasNUTS/ns72:level '0.0'^^xsd:decimal ;
        dct:temporal/dcat:startDate ?startDate ;
        dct:temporal/dcat:endDate ?endDate ;
        dcat:spatial/geo:hasGeometry/geo:asWKT ?location_geom ;
        ns71:accrualPeriodicity/rdfs:label ?freq ;
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
cursor.execute("DELETE FROM \"public\".\"dataset030\"")
connection.commit()

cursor.execute("DELETE FROM \"public\".\"dataset_period\" WHERE dataset_id = 30")
connection.commit()


#INSERT Data
for result in sparql.query().bindings:
    try:
        cursor.execute("INSERT INTO \"public\".\"dataset_period\" (\"dataset_id\", \"record_id\", \"start_period\", \"end_period\") VALUES ('"+result["datasetID"].value+"','"+result["id"].value+"','"+result["startDate"].value+"','"+result["endDate"].value+"')")
        connection.commit()

        cursor.execute("INSERT INTO \"public\".\"dataset030\" (\"record_id\", \"nuts_code\", \"dataset_id\", \"frequency\", \"measuredelement\", \"msrvalue\", \"msrunit\", \"location_geom\") VALUES ('"+result["id"].value+"','"+result["nutsID"].value+"','"+result["datasetID"].value+"','"+result["freq"].value+"','"+result["measuredElement"].value+"',"+result["msrValue"].value+",'"+result["msrUnit"].value+"','"+result["location_geom"].value+"')")
        connection.commit()
    except:
        print("Error with data: "+result)
cursor.close()
connection.close()

print("DONE")