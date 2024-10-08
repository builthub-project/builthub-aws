import psycopg2
from SPARQLWrapper import SPARQLWrapper2


#Conexion Graph
sparql = SPARQLWrapper2("http://localhost:7200/repositories/BuiltHub")


#Query
sparql.setQuery("""
PREFIX cbhsv: <http://data.builthub.eu/ontology/cbhsv#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX blthb: <http://data.builthub.eu/ontology/cbhsv#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX at: <http://publications.europa.eu/ontology/authority/>
PREFIX ns72: <http://data.europa.eu/nuts/>
SELECT ?identifier ?nutsID ?locationID ?startDate ?endDate ?msrValue ?measuredElement ?unit ?flags WHERE {
    ?s      a cbhsv:Dataset026 ;
        dc:identifier ?identifier ;
        blthb:availableFlag ?flags ;
        dct:temporal/dcat:startDate ?startDate ;
        dct:temporal/dcat:endDate ?endDate ;
        dcat:spatial/at:authority-code ?locationID ;
        cbhsv:hasNUTS/dc:identifier ?nutsID ;
        cbhsv:measuredElement/skos:prefLabel ?measuredElement ;
        cbhsv:measurementUnit ?unit ;
        cbhsv:measurementValue ?msrValue .
}
    """)


#Conexion Postgres
connection = psycopg2.connect("dbname=builthubdb user=user password=password host=localhost")
cursor = connection.cursor()


#DELETE Data
cursor.execute("DELETE FROM \"public\".\"dataset026\"")
connection.commit()

cursor.execute("DELETE FROM \"public\".\"dataset_period\" WHERE dataset_id = 26")
connection.commit()


#INSERT Data
for result in sparql.query().bindings:
    try:
        cursor.execute("INSERT INTO \"public\".\"dataset_period\" (\"dataset_id\", \"record_id\", \"start_period\", \"end_period\") VALUES ('26','"+result["identifier"].value+"','"+result["startDate"].value+"','"+result["endDate"].value+"')")
        connection.commit()
    
        cursor.execute("INSERT INTO \"public\".\"dataset026\" (\"record_id\", \"nuts_code\", \"location_id\", \"dataset_id\", \"msrvalue\", \"measuredelement\", \"msrunit\", \"flags\") VALUES ('"+result["identifier"].value+"','"+result["nutsID"].value+"','"+result["locationID"].value+"','26',"+result["msrValue"].value+",'"+result["measuredElement"].value+"','"+result["unit"].value+"','"+result["flags"].value+"')")
        connection.commit()
    except:
        print("Error with data: "+result)
cursor.close()
connection.close()

print("DONE")