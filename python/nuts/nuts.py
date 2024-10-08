import psycopg2
from SPARQLWrapper import SPARQLWrapper2


#Conexion Graph
sparql = SPARQLWrapper2("http://localhost:7200/repositories/BuiltHub")


#Query
sparql.setQuery("""
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX ns72: <http://data.europa.eu/nuts/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
SELECT DISTINCT ?code ?level ?name ?geom WHERE {
    ?s a skos:Concept ;
        dc:identifier ?code ;
        skos:prefLabel ?name ;
        ns72:level ?level .
        OPTIONAL{?s geo:hasGeometry/geo:asWKT ?geom .}

    FILTER (lang(?name) = 'en')
}
""")


#Conexion Postgres
connection = psycopg2.connect("dbname=builthubdb user=user password=password host=localhost")
cursor = connection.cursor()


#DELETE Data
cursor.execute("DELETE FROM \"public\".\"nuts\"")
connection.commit()


# avoids special characters in some region names
def format(value):
    result = ''
    if value == "Germany (until 1990 former territory of the FRG)":
        result = 'Germany'
    elif value == "Val-d'Oise":
        result = 'Val-d'+'\\'+'\'Oise'
    elif value == "Côte-d'Or":
        result = 'Côte-d'+'\\'+'\'Or'
    elif value == "Côtes-d'Armor":
        result = 'Côtes-d'+'\\'+'\'Armor'
    elif value == "Provence-Alpes-Côte d'Azur":
        result = 'Provence-Alpes-Côte d'+'\\'+'\'Azur'
    elif value == "Valle d'Aosta/Vallée d'Aoste":
        result = 'Valle d'+'\\'+'\'Aosta/Vallée d'+'\\'+'\'Aoste'
    elif value == "L'Aquila":
        result = 'L'+'\\'+'\'Aquila'
    elif value == "Reggio nell'Emilia":
        result = 'Reggio nell'+'\\'+'\'Emilia'
    elif value == "Agglomeratie 's-Gravenhage":
        result = 'Agglomeratie '+'\\'+'\'s-Gravenhage'
    else:
        result = value

    return result


#INSERT Data
for result in sparql.query().bindings:
    if(result["code"].value=="DE938" and result["level"].value=="2.0"):
        continue
    try:
        cursor.execute("INSERT INTO \"public\".\"nuts\" (\"nuts_code\", \"nuts_level\", \"name\",\"geomwkt\", \"geom\") VALUES ('"+result["code"].value+"',"+result["level"].value+",E'"+format(result["name"].value)+"','"+result["geom"].value+"','"+result["geom"].value+"')")
        connection.commit()
    except:
        try:
            cursor.execute("INSERT INTO \"public\".\"nuts\" (\"nuts_code\", \"nuts_level\", \"name\") VALUES ('"+result["code"].value+"',"+result["level"].value+",'"+result["name"].value+"')")
            connection.commit()
        except:
            print("Error with data: "+result)
cursor.close()
connection.close()

print("DONE")