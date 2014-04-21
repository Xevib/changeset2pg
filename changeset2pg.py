#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import psycopg2

def download_xml(min_lon,min_lat,max_lon,max_lat,extas):
    import urllib
    min_lon,min_lat,max_lon,max_lat
    url='http://www.openstreetmap.org/api/0.6/changesets?%s,%s,%s,%s'%(min_lon,min_lat,max_lon,max_lat)
    for key in extras.keys():
        if key=='user':
            url+=('?user=%s'%extras[key])
        elif key=='uid' or key=='user':
            url+=('?user=%s'%extras[key])
        elif key=='time':
            url+=('?time=%s'%extras[key])
        elif key=='time-range':
            url+=('?time=%s,%s'%(extras[key][0],extras[key][1]))
        elif key=='open':
            if extras[key]:
                url+='?open=ture'
            else:
                url+='?open=false'
        elif key=='closed':
            if extras[key]:
                url+='?closed=ture'
            else:
                url+='?closed=false'
        elif key=='changesets':
            url+='?changesets='
            for element in extras[key]:
                url+=str(element)+","
            if url[-1]==',':
                url=url[:-1]
    response=urllib.urlopen(url)
    xml = response.read()
    return xml
def create_table(host,db_username,db_password,database):
    

    conn = psycopg2.connect("dbname=%s user=%s password=%s host=%s"%(database,db_username,db_password,host))
    cur = conn.cursor()
    sql='CREATE TABLE changeset(id integer NOT NULL,"user" text NOT NULL,uid integer NOT NULL,created_at timestamp with time zone,closed_at timestamp with time zone,open boolean,area geometry,CONSTRAINT index PRIMARY KEY (id))WITH (OIDS=FALSE);'
    print sql
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
def import_data(database,db_username,db_password,host,xml):
    from lxml import etree
    
    conn = psycopg2.connect("dbname=%s user=%s password=%s host=%s"%(database,db_username,db_password,host))
    cur = conn.cursor()
    data=etree.fromstring(xml) 
    for change in data:
        sql="INSERT INTO changeset ("
        sql_values=" VALUES( "
        temp=(set(change.attrib.keys())-set(['min_lat','max_lat','min_lon','max_lon']))
        k=[]
        for key in temp:
            sql+='"'+str(key)+'",'
            if key=='id':
                sql_values+=str(change.attrib[key])+","
            elif key=='user':
                sql_values+=psycopg2.extensions.QuotedString(change.attrib[key].encode('utf-8')).getquoted()+","
            elif key=='uid':
                sql_values+=str(change.attrib[key])+","
            elif key=='created_at':
                sql_values+=psycopg2.extensions.QuotedString(change.attrib[key]).getquoted()+","
            elif key=='closed_at':
                sql_values+=psycopg2.extensions.QuotedString(change.attrib[key]).getquoted()+","
            elif key=='open':
                if str(change.attrib['open'])=='true':
                    sql_values+="'t',"
                else:
                    sql_values+="'f',"
        if 'min_lat' in change.attrib and 'max_lat' in change.attrib and 'min_lon' in change.attrib and 'max_lon' in change.attrib:
                sql_values+="ST_GeomFromText('POLYGON(("+str(change.attrib['min_lat'])+" "+str(change.attrib['min_lon'])+","+str(change.attrib['max_lat'])+" "+str(change.attrib['min_lon'])+","+str(change.attrib['min_lat'])+" "+str(change.attrib['max_lon'])+","+str(change.attrib['max_lat'])+" "+str(change.attrib['max_lon'])+","+str(change.attrib['min_lat'])+" "+str(change.attrib['min_lon'])+"))')"
                sql+="area,"
        
        while sql[-1]==',':
            sql=sql[:-1]
        while sql_values[-1]==',':
            sql_values=sql_values[:-1]
        sql+=")"
        sql_values+=");"
        sql+=sql_values
        print sql
        cur.execute(sql)
        conn.commit()
        
    cur.close()
    conn.close()
def print_help():
        print "Imports chagesets of an area to Postgresql database"
        print "Syntax:changeset2pg <host>  <db username> <db password> <database> <min_lat> <max_lat> <min_lon> <max_lon>"
        print "Extra arguments:"
        print "-user <username>"
        print "-time <start time> (YYYY-MM-DD)"
        print "-time-range <starttime> <end time>"
        print "-open <true/false>"
        print "-closed <true/false>"
        print "-changesets <changeset1,changeset2,ect>"
        exit()     
if __name__ == '__main__':
    if len(sys.argv)<9:
        print_help()
    else:
        host=sys.argv[1]
        db_username=sys.argv[2]
        db_password=sys.argv[3]
        database=sys.argv[4]
        min_lon=sys.argv[5]
        min_lat=sys.argv[6]
        max_lon=sys.argv[7]
        max_lat=sys.argv[8]
        x=9
        extras={}
        while(x<len(sys.argv)):
            if sys.argv[x]=='-user'and len(sys.argv)>=(x+1):
                extras['user']=sys.argv[x+1]
                x+=1
            elif sys.argv[x]=='-time' and len(sys.argv)>=(x+1):
                extras['time']=sys.argv[x+1]
                x+=1
            elif sys.argv[x]=='-time-range'and len(sys.argv)>=(x+2):
                extras['time-range']=[sys.argv[x+1],sys.argv[x+2]]
                x+=2
            elif sys.argv[x]=='-open'and len(sys.argv)>=(x+1) :
                extras['open']=sys.argv[x+1]
                x+=1
            elif sys.argv[x]=='-closed'and len(sys.argv)>=(x+1) :
                if sys.argv[x+1]=='true':
                    extras['']=True
                    x+=1
                elif sys.argv[x+1]=='false':
                    extras['closed']=False
                    x+=1
            elif sys.argv[x]=='-changesets' and len(sys.argv)>=(x+2):
                x+=1
                extras['changesets']=[sys.argv[x]]
                while sys.argv[x] not in ['user','time','time-range','open','closed']:
                    extras['changesets'].append(sys.argv[x])
                    x+=1
            else:
                print sys.argv[x]
                print_help()
            x+=1
        xml=download_xml(min_lon,min_lat,max_lon,max_lat,extras)
        #xml=download_xml(41.9737,2.7753,2.9101,42.0351,extras)
        create_table(host,db_username,db_password,database)
        import_data(database,db_username,db_password,host,xml)