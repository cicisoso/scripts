#!/usr/bin/env python
#encoding=utf8
## import MSSQL XML FILE to MYSQL
##
##
##
##  mouse.lew@gmail.com
##  6 July 2013
##

import sys,re,os,commands
import ConfigParser,json,logging
import warnings
from lxml import etree
import copy
import MySQLdb
import MySQLdb.constants
import MySQLdb.converters
import MySQLdb.cursors
import itertools
import logging
import argparse
warnings.filterwarnings("ignore")


"""A lightweight wrapper around MySQLdb."""

class Connection(object):
    """A lightweight wrapper around MySQLdb DB-API connections.

    The main value we provide is wrapping rows in a dict/object so that
    columns can be accessed by name. Typical usage:

        db = database.Connection("localhost", "mydatabase")
        for article in db.query("SELECT * FROM articles"):
            print article.title

    Cursors are hidden by the implementation, but other than that, the methods
    are very similar to the DB-API.

    We explicitly set the timezone to UTC and the character encoding to
    UTF-8 on all connections to avoid time zone and encoding errors.
    """
    def __init__(self, host, database, user=None, password=None , prefix = ""):
        self.host = host
        self.database = database
        self.prefix = prefix

        args = dict(conv=CONVERSIONS, use_unicode=True, charset="utf8",
                    db=database, init_command='SET time_zone = "+8:00"',
                    sql_mode="TRADITIONAL")
        if user is not None:
            args["user"] = user
        if password is not None:
            args["passwd"] = password

        # We accept a path to a MySQL socket file or a host(:port) string
        if "/" in host:
            args["unix_socket"] = host
        else:
            self.socket = None
            pair = host.split(":")
            if len(pair) == 2:
                args["host"] = pair[0]
                args["port"] = int(pair[1])
            else:
                args["host"] = host
                args["port"] = 3306

        self._db = None
        self._db_args = args
        try:
            self.reconnect()
        except:
            logging.error("Cannot connect to MySQL on %s", self.host,
                          exc_info=True)
            raise Exception("connection to mysql failed, please check your configure")

    def __del__(self):
        self.close()

    def close(self):
        """Closes this database connection."""
        if self._db is not None:
            self._db.close()
            self._db = None

    def commit(self):
        if self._db is not None:
            try:
                self._db.ping()
            except:
                self.reconnect()
            try:
                self._db.commit()
            except Exception,e:
                self._db.rollback()
                logging.exception("Can not commit",e)

    def rollback(self):
        if self._db is not None:
            try:
                self._db.rollback()
            except Exception,e:
                logging.error("Can not rollback")

    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        self.close()
        self._db = MySQLdb.connect(**self._db_args)
        self._db.autocommit(True)


    def iter(self, query, *parameters):
        """Returns an iterator for the given query and parameters."""
        if self._db is None: self.reconnect()
        cursor = MySQLdb.cursors.SSCursor(self._db)
        try:
            self._execute(cursor, query, parameters)
            column_names = [d[0] for d in cursor.description]
            for row in cursor:
                yield Row(zip(column_names, row))
        finally:
            cursor.close()

    def query(self, query, *parameters):
        """Returns a row list for the given query and parameters."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters)
            column_names = [d[0] for d in cursor.description]
            return [Row(itertools.izip(column_names, row)) for row in cursor]
        finally:
            cursor.close()



    def get(self, query, *parameters):
        """Returns the first row returned for the given query."""
        rows = self.query(query, *parameters)
        if not rows:
            return None
        elif len(rows) > 1:
            raise Exception("Multiple rows returned for Database.get() query")
        else:
            return rows[0]

    def execute(self, query, *parameters):
        """Executes the given query, returning the lastrowid from the query."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters)
            if cursor.lastrowid > 0:
                return cursor.lastrowid
            return cursor.rowcount
        finally:
            cursor.close()
    def count(self,query, *parameters):
        """Executes the given query, returning the count value from the query."""
        cursor = self._cursor()
        try:
            cursor.execute(query, parameters)
            return cursor.fetchone()[0]
        finally:
            cursor.close()

    def __getattr__(self,tablename):
        '''
        return single table queryer for select table
        '''
        return TableQueryer(self, self.prefix + tablename)

    def fromQuery(self,Select):
        '''
        return single table queryer for query
        '''
        return TableQueryer(self,Select)

    def insert(self,table,**datas):
        '''
        Executes the given parameters to an insert SQL and execute it
        '''
        return Insert(self,table)(**datas)

    def executemany(self, query, parameters):
        """Executes the given query against all the given param sequences.

        We return the lastrowid from the query.
        """
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def _cursor(self):
        if self._db is None: self.reconnect()
        try:
            self._db.ping()
        except:
            self.reconnect()
        return self._db.cursor()

    def showtables(self, base_name):
        cursor = self._cursor()
        base_name = base_name + '%'
        sql = "SHOW TABLES LIKE %s "
        self._execute( cursor , sql , (base_name,) )
        tables = {}
        for t in cursor:
            tables[t[0]] = set()
        # get the fields for each table
        sql = "SHOW COLUMNS FROM `%s`"
        #sql = "SELECT 1 FROM `%s`"
        for t in tables:
            cursor = self._cursor()
            self._execute( cursor , sql % t , () )
            for f in cursor:
                tables[t].add(f[0])
        return tables

    def _execute(self, cursor, query, parameters):
        try:
            return cursor.execute(query , parameters)
        except OperationalError,e:
            #logging.error("Error connecting to MySQL on %s", self.host)
            logging.error("Error: %s" , e.message)
            print query
            self.close()
            raise

# Fix the access conversions to properly recognize unicode/binary
FIELD_TYPE = MySQLdb.constants.FIELD_TYPE
FLAG = MySQLdb.constants.FLAG
CONVERSIONS = copy.deepcopy(MySQLdb.converters.conversions)
for field_type in \
        [FIELD_TYPE.BLOB, FIELD_TYPE.STRING, FIELD_TYPE.VAR_STRING] + \
        ([FIELD_TYPE.VARCHAR] if 'VARCHAR' in vars(FIELD_TYPE) else []):
    CONVERSIONS[field_type].insert(0, (FLAG.BINARY, str))

# Alias some common MySQL exceptions
IntegrityError = MySQLdb.IntegrityError
OperationalError = MySQLdb.OperationalError


## read schema xml descrp and create table ddl to create table
def read_xml_schema(schema):
    type_map = {
                'xs:string' : 'varchar(128) DEFAULT ""',
                'xs:decimal' : 'decimal(25,5) DEFAULT 0.0',
                'xs:int' : 'int(11) DEFAULT 0',
                'xs:boolean' : ' varchar(10) DEFAULT "" NOT NULL'}
    table = {}
    table['table'] = schema.attrib['id']
    fields = {}
    for child in  schema.find('*/*/*/*/*/*'):
        fields [ child.attrib['name'] ] = {}
        fields [ child.attrib['name'] ][ 'name' ] = child.attrib['name']
        type = child.attrib['type']
        if type in type_map:
            fields[ child.attrib['name'] ] = type_map[ type ]


    table['fields'] = fields

    #create sql for this table
    #`word_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
    #`word` varchar(32) NOT NULL,
    #`weight` smallint(6) NOT NULL DEFAULT ''0'',
    sql = 'CREATE TABLE IF NOT EXISTS `__TABLE__` ( `id` int(11) unsigned NOT NULL AUTO_INCREMENT , %s , PRIMARY KEY (`id`) ) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8'

    ddl = []
    for f in fields :
        tmp = '`%s` %s NOT NULL'
        ddl.append( tmp % ( f , fields[f] ) )

    # try to create database
    create_ddl = sql % ",".join( ddl )

    return { 'tablename' : table['table'] , 'ddl' : create_ddl , 'fields' : fields}


def import_to_mysql(db , filepath):
    # read the xml file
    tree = etree.parse(filepath)
    base_table = os.path.split(os.path.dirname(filepath))[1]
    root = tree.getroot()
    exists_tables = db.showtables(base_table)
    ns = {
            'xs' : 'http://www.w3.org/2001/XMLSchema',
            'msdata' : 'urn:schemas-microsoft-com:xml-msdata' }
    # create table if it doesn't exists
    schema = root.findall('xs:schema' , namespaces=ns)
    table = {}
    target_table = ''
    if len(schema) :
        schema = schema[0]
        table_info = read_xml_schema(schema)
        fields = set( table_info['fields'].keys() )
        for t in exists_tables:
            # found one talbe for this data : continue
            if ( len( exists_tables[t] & fields ) == len(fields) ) :
                target_table = t
                new_table_name = t

        # if no table match the data , try create it
        if not target_table:
            # define the new table name based on dictionary path
            print 'creating new table for this xml file'
            check = 1
            num = 1
            new_table_name = base_table
            while check:
                if new_table_name in exists_tables:
                    new_table_name = "%s_%d" % (base_table , num)
                    num += 1
                else :
                    check = 0

            try :
                res = db.query('SELECT 1 FROM `%s`' %  new_table_name)
            except :
                print 'table `%s` does not exist , creating it ...' % new_table_name
                create_sql = table_info['ddl']
                create_sql = create_sql.replace("__TABLE__" , new_table_name)
                db.execute( create_sql )

    else:
        print "can not find any schema description in this xml file %s " % filepath
        return

    print 'importing from %s ...' % filepath
    # begin to read data rows
    element = schema.find('*/*/*/')
    row_name = element.attrib['name']
    succ_num = 0
    fail_num = 0
    total_num = 0
    for row in root.findall(row_name):
        fields = []
        values = []
        total_num += 1
        for f in row:
            #data[f.tag] = f.text
            fields.append(f.tag)
            if f.text :
                text = f.text
            else :
                text = ""
            values.append( '"' + text + '"')

        sql = 'INSERT INTO %s ( %s ) VALUES ( %s )'
        fields = ",".join(fields)
        values = ",".join(values)
        sql = sql % (  new_table_name , fields , values )
        try :
            affected_num = db.execute(sql)
        except :
            affected_num = 0

        if affected_num :
            succ_num += 1
        else:
            fail_num += 1

    print "imported from `%s` , total : %d records , succ : %d records, fail : %d records " % ( filepath , total_num , succ_num , fail_num )

if __name__ == "__main__":

    options = {}
    conf_file = os.path.abspath( os.path.join( os.path.split(os.path.realpath(__file__))[0] , "db.conf" ) )
    cf = ConfigParser.ConfigParser()
    cf.read(conf_file)
    parser = argparse.ArgumentParser(description='This is a little tool for Stan to import xml to mysqli')
    parser.add_argument('-o','--host', help='mysql server address',required=True)
    parser.add_argument('-t','--db',help='mysql database  name', required=True)
    parser.add_argument('-u','--username',help='mysql user name', required=True)
    parser.add_argument('-p','--password',help='mysql user password', required=True)
    parser.add_argument('-d','--datadir',help='the absolute path of the the xml data file of the data dictionary', required=True)
    args = parser.parse_args()


    # initial config
    '''
    for section in cf.sections():
        options[section] = dict(cf.items(section))
    '''

    # initial db
    db=Connection(host=args.host,database=args.db, user=args.username, password=args.password)



    #check if it's a file or a dictionary
    xml_files = []
    if  os.path.isfile( args.datadir ) :
        xml_files.append( args.datadir )

    if os.path.isdir( args.datadir):
        for root, dirs, files in os.walk( args.datadir):
            for file in files:
                if file.endswith('.xml'):
                    xml_files.append( os.path.abspath(os.path.join( root , file )) )

    if not len(xml_files):
        print 'can not find any xml files in "%s"' % args.datadir
        sys.exit(1)

    print 'found %d xml files , begin to import ...' % len( xml_files )

    for f in xml_files:
        print "begin importing of file : %s" % f
        import_to_mysql(db , f )
