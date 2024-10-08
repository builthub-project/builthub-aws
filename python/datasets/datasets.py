import psycopg2
from SPARQLWrapper import SPARQLWrapper2


#Conexion Graph
sparql = SPARQLWrapper2("http://localhost:7200/repositories/BuiltHub")


#Query
sparql.setQuery("""
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
prefix xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
SELECT DISTINCT ?id ?name ?description
WHERE {
    ?s rdf:type/rdfs:label ?name ;
       rdf:type/rdfs:comment ?description ;
       rdf:type ?var ;
       FILTER(contains(str(?var),'https://data.builthub.eu/ontology/cbhsv#Dataset'))
    BIND(STRAFTER(str(?var),'https://data.builthub.eu/ontology/cbhsv#Dataset') as ?id)
}
    """)


#Conexion Postgres
connection = psycopg2.connect("dbname=builthubdb user=user password=password host=localhost")
cursor = connection.cursor()


#DELETE Data
cursor.execute("DELETE FROM \"public\".\"datasets\"")
connection.commit()


#INSERT Data
for result in sparql.query().bindings:
    if(result["id"].value==""):
        continue
    try:
        cursor.execute("INSERT INTO \"public\".\"datasets\" (\"dataset_id\", \"name\", \"description\") VALUES ('"+result["id"].value.lstrip('0')+"','"+result["name"].value+"','"+result["description"].value.replace("'"," ")+"')")
        connection.commit()
    except:
        print("Error with data: "+result)
cursor.execute("INSERT INTO \"public\".\"datasets\" (\"dataset_id\", \"name\", \"description\") VALUES (501,'EUROSTAT: Households by size (persons)','This dataset contains data about the number of households by number of persons living and the average persons by household.')")
connection.commit()
cursor.close()
connection.close()

print("DONE")