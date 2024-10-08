import psycopg2
from SPARQLWrapper import SPARQLWrapper2


#Conexion Graph
sparql = SPARQLWrapper2("http://localhost:7200/repositories/BuiltHub")


#Query
sparql.setQuery("""
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT DISTINCT ?notation ?name WHERE {
    ?s a skos:Concept ;
       	skos:inScheme <http://dd.eionet.europa.eu/vocabulary/eurostat/siec/> ;
        skos:notation ?notation ;
        skos:prefLabel ?name .
}
    """)


#Conexion Postgres
connection = psycopg2.connect("dbname=builthubdb user=user password=password host=localhost")
cursor = connection.cursor()


#DELETE Data
cursor.execute("DELETE FROM \"public\".\"siec\"")
connection.commit()


#INSERT Data
for result in sparql.query().bindings:
    try:
        cursor.execute("INSERT INTO \"public\".\"siec\" (\"siec_id\", \"name\") VALUES ('"+result["notation"].value+"','"+result["name"].value+"')")
        connection.commit()
    except:
        print("Error with data: "+result)
cursor.close()
connection.close()

print("DONE")