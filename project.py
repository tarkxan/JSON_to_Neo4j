'''
Created on Sep 26, 2018

@author: Lando
'''

#!/usr/bin/python3

"""Insert data from collectData.py into a neo4j database and query it.
"""

import glob
import json
import neo4j.v1
import neo4j.exceptions
import os.path
import sys

''' ------------------------------------------     
            queryNeo4j       
------------------------------------------ '''
def queryNeo4j(session):
    
    # 0. Count the total number of images in the database
    query_0 = '''
    MATCH (n:ImageLbl) RETURN count(n) as cnt
    '''
    queryNeo4jAndPrintResults(query_0, 
                              session, 
                              title = 'Query 0')
    
    # 1. Count the total number of JSON documents in the database
    query_1 = '''
    MATCH(i:ImageLbl) WHERE i.isDocument = true RETURN count(1)
    '''
    
    queryNeo4jAndPrintResults(query_1, 
                              session, 
                              title = 'Query 1')

    # 2. Count the total number of Images, Labels, Landmarks, 
    # Locations, Pages, and WebEntity's in the database
    query_2 = '''
    MATCH(i:ImageLbl) RETURN 'Images:\t' as i, count(1) 
    union all 
    MATCH(l:LabelLbl) RETURN 'Labels:\t' as i, count(1)
    union all
    MATCH (p:PageLbl) RETURN 'Pages:\t' as i, count(1)
    union all
    match (l:LandmarkLbl) return 'Landmarks:\t' as i, count(1)
    union all
    MATCH (n:LocationLbl) RETURN 'Locations:\t' as i, count(1)
    union all
    MATCH (w:WenEntityLbl) RETURN 'Web Entities:' as i, count(1)
    '''
    
    queryNeo4jAndPrintResults(query_2, 
                              session, 
                              title = 'Query 2')
    
    # 3. List all of the Images that are associated with the
    # Label with an id of "/m/015kr" (which has the description
    # "bridge") ordered by the score of the association between them
    # from highest to lowest
    query_3 = '''
    match (i:ImageLbl)- [c:contains] -> (l:LabelLbl) 
    where l.mid = '/m/015kr' 
      and l.description = 'bridge' 
    return c.score, 
           i.url, 
           i.isDocument
    order by c.score desc
    '''
    
    queryNeo4jAndPrintResults(query_3, 
                              session, 
                              title = 'Query 3')
    
    # 4. List the 10 most frequent WebEntitys that are applied
    # to the same Images as the Label with an id of "/m/015kr" (which
    # has the description "bridge"). List them in descending order of
    # the number of times they appear, followed by their entityId
    # alphabetically
    query_4 = '''
    match(l:LabelLbl)<-[c:contains]-(i:ImageLbl)-[itw:image_tagged_webEntity]->(w:WenEntityLbl) 
    where l.mid = '/m/015kr' 
      and l.description = 'bridge' 
    return count(1), 
           w.entityId, 
           w.description 
    order by count(1) desc, 
            w.entityId asc 
    limit 10
    '''
    
    queryNeo4jAndPrintResults(query_4, 
                              session, 
                              title = 'Query 4')
    
    # 5. Find Images associated with Landmarks that are not "New
    # York" (id "/m/059rby") or "New York City" (id "/m/02nd ")
    # ordered alphabetically by landmark description and then by image
    # URL.
    query_5 = '''
    match(l:LandmarkLbl)<-[c:image_contains_landmark]-(i:ImageLbl) 
    where not l.mid in['/m/059rby', 
                       '/m/02nd_'] 
    return i.url as image_url, 
           l.description as landmark 
    order by l.description, 
          i.url asc
    '''
    queryNeo4jAndPrintResults(query_5, 
                              session, 
                              title = 'Query 5')

    # 6. List the 10 Labels that have been applied to the most
    # Images along with the number of Images each has been applied to
    query_6 = '''
    match(i:ImageLbl)-[c:contains]->(l:LabelLbl) 
    return l.mid         as mid, 
           l.description as label, 
           count(1)      as num_of_images 
    order by count(1) desc 
    limit 10
    '''
    
    queryNeo4jAndPrintResults(query_6, 
                              session, 
                              title = 'Query 6')
    
    # 7. List the 10 Pages that are linked to the most Images
    # through the webEntities.pagesWithMatchingImages JSON property
    # along with the number of Images linked to each one. 
    # Sort them by count (descending) and then by page URL.
    query_7 = '''
    match(i:ImageLbl)-[iop:image_on_page]->(p:PageLbl) 
    return p.url    as page,
           count(1) as num_of_images
    order by count(1) desc, 
             p.url asc  
    limit 10
    '''
    
    queryNeo4jAndPrintResults(query_7, 
                              session, 
                              title = 'Query 7')
    
    # 8. List the 10 pairs of Images that appear on the most Pages together
    # through the webEntities.pagesWithMatchingImages JSON property.
    # Order them by the number of pages that they appear on together (descending),
    # then by the URL of the first.
    # Make sure that each pair is only listed once regardless
    # of which is first and which is second.
    
    query_8 = '''
    match(i:ImageLbl)-[iop:image_on_page]->(p:PageLbl)<-[iop2:image_on_page]-(ii:ImageLbl) 
    where i.isDocument = true 
      and ii.isDocument = true
      and i.url < ii.url 
    return count(p.url),
           i.url, 
           ii.url 
    order by count(p.url) desc, 
             i.url asc 
    limit 10
    '''
    
    queryNeo4jAndPrintResults(query_8, 
                              session, 
                              title = 'Query 8')
    
''' ------------------------------------------     
            queryNeo4jAndPrintResults       
------------------------------------------ '''
def queryNeo4jAndPrintResults(query, 
                              session, 
                              title = "Running query:"):
    print()
    print(title)
    print(query)

    if not query.strip():
        return
    
    for record in session.run(query):
        
        print(' ' * 4, end = ' ')
        #print('record: ', record)
        
        for field in record:
            
            #print(record[field], end="\t")
            print(field, end = '\t\t') 
        
        print()

''' ------------------------------------------     
            insertFileData       
------------------------------------------ '''        
def insertFileData(session, 
                   jsonData):
    
    # proceed all necessary entities and relationships at once
    insertImage = '''
    with {json} as data
    unwind data as q 
    merge (i: ImageLbl {url: q.url})
      on create set i.isDocument = true
      on match  set i.isDocument = true
      
    foreach (row in q.response.labelAnnotations |
        merge (l: LabelLbl {mid: row.mid})
            on create set l.description = row.description
            
        merge (i) - [cont:contains {score:row.score}] -> (l)
            on create set cont.score = row.score
    )
        
    foreach (row in q.response.webDetection.fullMatchingImages |
        merge (imi: ImageLbl {url: row.url})
            on create set imi.isDocument = false
            
        merge (i) - [m:matches] -> (imi)
            on create set m.type = 'full'
            on match set m.type = 'full'
    )
    
    foreach (row in q.response.webDetection.partialMatchingImages |
        merge (pm: ImageLbl {url: row.url})
            on create set pm.isDocument = false
            
        merge (i) - [m: matches] -> (pm)
            on create set m.type = 'partial'
    )
            
    foreach (row in q.response.webDetection.pagesWithMatchingImages |
        merge (p: PageLbl {url: row.url})
        
        merge (i) - [iop: image_on_page] -> (p)
    )
        
    foreach (row in q.response.webDetection.webEntities |
        merge (we: WenEntityLbl {entityId: row.entityId})
            on create set we.description = row.description
            
        merge (i) - [itwe: image_tagged_webEntity] -> (we)
            on create set itwe.score = row.score
    )
            
    foreach (row in q.response.landmarkAnnotations |
        merge (l: LandmarkLbl {mid: row.mid, description: coalesce(row.description, row.mid)})
            
        merge (i) - [icl: image_contains_landmark] -> (l)
            on create set icl.score = row.score
        
        foreach (srow in row.locations |
        
            foreach (ssrow in srow.latLng |
        
                merge (loc: LocationLbl {latitude: ssrow.latitude, longitude: ssrow.longitude})
                
                merge (l) - [lal: located_at_location] -> (loc)
            )
        ) 
    )
    '''
       
    loadedImages = 0
    
    try:
        session.run(insertImage, 
                    {"json": jsonData})
        
        loadedImages += 1
    
    except neo4j.exceptions.ClientError as ce:
        print(f'\t*******\n\tWARNING: {str(ce)}\n\t*******')  

''' ------------------------------------------     
            populateNeo4j
    
Load the JSON results from google into neo4j      
------------------------------------------ '''        
def populateNeo4j(session,
                  jsonDir, 
                  clearDb = False):
    
    "Load the JSON results from google into neo4j"

    # From: https://stackoverflow.com/a/29715865/2037288
    deleteQuery = '''
    match (n)
    optional match (n)-[r]-()
    with n, r LIMIT 50000
    delete n, r
    return count(n) as deletedNodesCount
    '''
    
    if clearDb:
       
        result = session.run(deleteQuery)
        
        for record in result:
            
            print('Deleted', record['deletedNodesCount'], 'nodes')

    loadedFiles = 0
    
    for jsonFile in glob.glob(os.path.join(jsonDir, '*.json')):
        
        print("Loading", jsonFile, "into neo4j")
        
        with open(jsonFile) as jf:
            
            jsonData = json.load(jf)
            
            insertFileData(session, 
                           jsonData)
            
            loadedFiles += 1
            
    print("\nLoaded", loadedFiles, "JSON files into Neo4j\n")
    
    countQuery = '''
    MATCH (a) WITH DISTINCT LABELS(a) AS temp, COUNT(a) AS tempCnt
    UNWIND temp AS label
    RETURN label, SUM(tempCnt) AS cnt
    ORDER BY label
    '''
    
    queryNeo4jAndPrintResults(countQuery, 
                              session, 
                              "Neo4j now contains")
    
''' ---------------------     
            main  
--------------------- '''
def main():
    
    #jsonDir = 'E:\\Neo4jProject\\data\json\\'
    jsonDir = os.path.join(sys.argv[1], "json")
    uri = 'bolt://localhost:7687'
    dbName = 'neo4j'
    password = 'cisc7610'
    
    driver = neo4j.v1.GraphDatabase.driver(uri, 
                                           auth=neo4j.v1.basic_auth(dbName, 
                                                                    password))
    session = driver.session()
    
    populateNeo4j(session,
                  jsonDir, 
                  True)    # clearDb
    
    # fetch data from schema tables
    queryNeo4j(session)
    
    session.close()          
        
if __name__ == '__main__':
    main()
