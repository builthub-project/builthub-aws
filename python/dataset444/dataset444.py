import psycopg2
from SPARQLWrapper import SPARQLWrapper2


#Conexion Graph
sparql = SPARQLWrapper2("http://localhost:7200/repositories/BuiltHub")


#Query
sparql.setQuery("""
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX cbhsv: <http://data.builthub.eu/ontology/cbhsv#>
PREFIX ns72: <http://data.europa.eu/nuts/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX at: <http://publications.europa.eu/ontology/authority/>
SELECT DISTINCT ?identifier ?startDate ?endDate ?location ?location_id ?nutsID ?nutsLevel ?freq ?topic ?feature ?ttype ?msrUnit ?msrValue
WHERE {
    ?s      a cbhsv:Dataset444 ;
        cbhsv:hasNUTS/ns72:level '0.0'^^xsd:decimal ;
        dc:identifier ?identifier ;
        dct:temporal/dcat:startDate ?startDate ;
        dct:temporal/dcat:endDate ?endDate ;
        dcat:spatial/skos:prefLabel ?location ;
        dcat:spatial/at:authority-code ?location_id ;
        cbhsv:hasNUTS/dc:identifier ?nutsID ;
        cbhsv:hasNUTS/ns72:level ?nutsLevel ;
        dcterms:accrualPeriodicity ?freq ;
        cbhsv:topic/skos:prefLabel ?topic ;
        cbhsv:feature/skos:prefLabel ?feature ;
        cbhsv:type/skos:prefLabel ?ttype ;
        cbhsv:measurementUnit ?msrUnit;
        cbhsv:measurementValue ?msrValue .
    FILTER (lang(?location) = 'en')
}
""")


#Conexion Postgres
connection = psycopg2.connect("dbname=builthubdb user=user password=password host=localhost")
cursor = connection.cursor()


#DELETE Data
cursor.execute("DELETE FROM \"public\".\"dataset444\"")
connection.commit()

cursor.execute("DELETE FROM \"public\".\"dataset_period\" WHERE dataset_id = 444")
connection.commit()


#INSERT Data
for result in sparql.query().bindings:
    try:
        cursor.execute("INSERT INTO \"public\".\"dataset_period\" (\"dataset_id\", \"record_id\", \"start_period\", \"end_period\") VALUES ('444','"+result["identifier"].value+"','"+result["startDate"].value+"','"+result["endDate"].value+"')")
        connection.commit()

        cursor.execute("INSERT INTO \"public\".\"dataset444\" (\"record_id\", \"nuts_code\", \"location_id\", \"dataset_id\", \"topic\", \"feature\", \"topic_type\", \"msrvalue\", \"msrunit\") VALUES ('"+result["identifier"].value+"','"+result["nutsID"].value+"','"+result["location_id"].value+"','444','"+result["topic"].value+"','"+result["feature"].value+"','"+result["ttype"].value+"',"+result["msrValue"].value+",'"+result["msrUnit"].value+"')")
        connection.commit()
    except:
        print("Error with data: "+result)
cursor.close()
connection.close()

print("DONE")