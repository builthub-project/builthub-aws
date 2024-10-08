import psycopg2
from SPARQLWrapper import SPARQLWrapper2


#Conexion Graph
sparql = SPARQLWrapper2("http://localhost:7200/repositories/BuiltHub")


#Query
sparql.setQuery("""
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX cbhsv: <http://data.builthub.eu/ontology/cbhsv#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX at: <http://publications.europa.eu/ontology/authority/>
SELECT DISTINCT ?identifier ?NutsId ?LocationId ('501'^^xsd:integer as ?datasetID) ?startDate ?endDate ?msrValue ?measuredElement ?unit 
WHERE {
    ?s      a cbhsv:Dataset501 ;
        dc:identifier ?identifier ;
        dct:temporal/dcat:startDate ?startDate ;
        dct:temporal/dcat:endDate ?endDate ;
        dcat:spatial/at:authority-code ?LocationId ;
        cbhsv:hasNUTS/dc:identifier ?NutsId ;
        cbhsv:measuredElement ?measuredElement ;
        cbhsv:measurementUnit ?unit ;
        cbhsv:measurementValue ?msrValue .
}
    """)


#Conexion Postgres
connection = psycopg2.connect("dbname=builthubdb user=user password=password host=localhost")
cursor = connection.cursor()


#DELETE Data
cursor.execute("DELETE FROM \"public\".\"dataset501\"")
connection.commit()

cursor.execute("DELETE FROM \"public\".\"dataset_period\" WHERE dataset_id = 501")
connection.commit()


#INSERT Data
for result in sparql.query().bindings:
    try:
        cursor.execute("INSERT INTO \"public\".\"dataset_period\" (\"dataset_id\", \"record_id\", \"start_period\", \"end_period\") VALUES ('"+result["datasetID"].value+"','"+result["identifier"].value+"','"+result["startDate"].value+"','"+result["endDate"].value+"')")
        connection.commit()

        cursor.execute("INSERT INTO \"public\".\"dataset501\" (\"record_id\", \"nuts_code\", \"location_id\", \"dataset_id\", \"msrvalue\", \"measuredelement\", \"msrunit\") VALUES ('"+result["identifier"].value+"','"+result["NutsId"].value+"','"+result["LocationId"].value+"','"+result["datasetID"].value+"',"+result["msrValue"].value+",'"+result["measuredElement"].value+"','"+result["unit"].value+"')")
        connection.commit()
    except:
        print("Error with data: "+result)
cursor.close()
connection.close()

print("DONE")