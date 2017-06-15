# Python 2 code to fetch data from EDDB in a reasonably friendly manner

#import zlib, urllib2, json, csv

# ... EDMC includes Requests. Use it. 
# It would be possible to use urllib, decode via zlib, etc. 
# Requests does it by default and on the fly. 
# Only etag caching remains. 
# Add an "If-None-Match" header with the received ETag value.
# Check result code for 304 Not Modified. 


import sqlite3, os, json, csv, sys, itertools, codecs
import pprint, datetime

# Uses unix timestamps in database, because SQLite actually stores them as 
# ISO 8601 strings. That's great for human readability, lousy for size. 
# Could go one further and use E:D epoch instead of unix epoch, 
# or two further and just not store values we don't yet use. 
# But market data timestamps are a popular thing to show and even filter on. 

# 0:00:00.001000
# Loading systems (populated)
# 21200125/21200125
# 0:00:02.129000
# 0:00:00
# 0:00:00.075000
    # 0/395840:00:00.004000
# Loading factions
# 6393391/6393391
# 0:00:01.041000
# Loading stations
# 107498097/107498097
# 0:00:07.272000
# Loading market data
# 125639983/125639983
# 0:00:32.448000
# 0:00:00
# Enabling journal... enabling indices... vacuuming... analyzing... done.
# 0:00:03.791000


class Progress(object):
    "Tracks progress reading a file through line iteration"
    __slots__ = ("end", "file", "fmt", "iterator", "pos")
    def __init__(self, file):
        self.file = file
        pos = file.tell()
        file.seek(0, os.SEEK_END)
        self.end = file.tell()
        file.seek(pos, os.SEEK_SET)
        self.fmt = "\r{{:{}}}/{{}}".format(len(str(self.end)))
        self.iterator = iter(file)
        self.pos = 0
    def __iter__(self):
        return self
    def next(self):
        sys.stdout.write(self.fmt.format(self.pos,self.end))
        #sys.stdout.flush()
        val = next(self.iterator)
        self.pos += len(val)
        return val
    def read(self, size=None):
        sys.stdout.write(self.fmt.format(self.pos,self.end))
        if size is None:
            data = self.file.read()
        else:
            data = self.file.read(size)
        self.pos += len(data)
        return data
        

class Column(object):
    __slots__=('sqlname','csvname','csvcolumn','sqltype')
    def __init__(self, defs):
        try:
            sqlname,csvname,sqltype=defs
        except ValueError, e:
            sqlname,sqltype=defs
            csvname=sqlname
        self.sqlname=sqlname
        self.csvname=csvname
        self.sqltype=sqltype
        #self.csvcolumn=column
class CSVtoSQL(object):
    __slots__=('columns',)
    def __init__(self, csv, coldefs):
        csvcolumns=next(csv)
        columns=map(Column, coldefs)
        for col in columns:
            col.csvcolumn = csvcolumns.index(col.csvname)
        self.columns=sorted(columns, key=lambda c: c.csvcolumn)
    def sqlcoldefs(self):
        return ','.join(c.sqlname+" "+c.sqltype for c in self.columns)
    def sqlcolumns(self):
        return ','.join(c.sqlname for c in self.columns)
    def qmarks(self):
        return ','.join("datetime(?,'unixepoch')" if c.sqltype=="datetime" 
                        else "nullif(?,'')" 
                        for c in self.columns)
    def createtable(self,cursor,table,temporary=True):
        cursor.execute("create {} table {}({})".format(
                        "temporary" if temporary else "",
                        table, self.sqlcoldefs()))
    def insertinto(self,cursor,table,csv):
        # FIXME: Failed here with an error trying to encode a unicode as ascii. Why?
        def decode(val):
            # Python 2 csv module doesn't pass unicode (produces str)
            # and sqlite3 module doesn't accept utf8 str
            # unless configured to output it using conn.text_factory=str
            # We could do that during import. Would save us some calls. 
            if isinstance(val,str):
                return val.decode('utf-8')
            else:
                return val
        cursor.executemany("insert into {}({}) values ({})".format(
                table, self.sqlcolumns(), self.qmarks()), 
                (map(decode, row) for row in csv))

def spatialindex(x,y,z):
    "Calculates an index value preserving 3D locality"
    index = 0
    # Same scaling for each axis so the subdivisions are the same magnitude (cubic cells)
    x = int(x*((1<<20)/2e5)+(1<<19))
    y = int(y*((1<<20)/2e5)+(1<<19))
    z = int(z*((1<<20)/2e5)+(1<<19))
    for bit in range(19,-1,-1):     # count from MSB
        # Allocate groups of bits for same weight coordinate bits
        index |= ((x>>bit)&1)
        index |= ((y>>bit)&1)<<1
        index |= ((z>>bit)&1)<<2
        index <<= 3
    return index

class EDDB(object):
    __slots__ = ("conn", "cursor")
    def __init__(self, connection):
        # Given a database connection, ensures EDDB data schema is in there.
        self.conn = connection
        self.conn.row_factory = sqlite3.Row
        self.cursor = connection.cursor()
        # Ensure we have the appropriate tables
        moduledir = os.path.dirname(__file__)
        table_defs = open(os.path.join(moduledir, "tables.sql")).read()
        # This silly loop can be replaced with executescript. Oh well. 
        # This just might make it easier to debug by showing the failed statement.
        for table_def in table_defs.split(';'):
            try:
                self.cursor.execute(table_def)
            except:
                print table_def
                raise
        self.conn.commit()

        self.conn.create_function("spatialindex", 3, spatialindex)

        #try:
        #    self.cursor.execute('''alter table eddb_minor_factions 
        #                   add home_system_id integer references eddb_systems(id)''')
        #    self.conn.commit()
        #except sqlite3.OperationalError, e:
        #    if e.message != "duplicate column name: home_system_id":
        #        raise

    def disable_indices(self, name=None):
        moduledir = os.path.dirname(__file__)
        defs = open(os.path.join(moduledir, "indices.sql")).read()
        stmt = "create index if not exists"
        for idx_def in defs.split(';'):
            if name and name not in idx_def:
                continue
            try:
                start = idx_def.index(stmt)
            except ValueError:
                continue    # last string is empty anyway
            thisname = idx_def[start+len(stmt):].split(None,1)[0]
            try:
                self.cursor.execute("drop index if exists " + thisname)
            except:
                print table_def
                raise

    def enable_indices(self, name=None):
        moduledir = os.path.dirname(__file__)
        defs = open(os.path.join(moduledir, "indices.sql")).read()
        for idx_def in defs.split(';'):
            if name and name not in idx_def:
                continue
            try:
                self.cursor.execute(idx_def)
            except:
                print table_def
                raise
        
    
    def __del__(self):
        #self.cursor.execute("pragma optimize")
        pass

    def debug_query(self, query, *args, **kwargs):
        c = self.conn.cursor()
        c.execute(query, *args, **kwargs)
        pprint.pprint([{k:v for k,v in zip(r.keys(),r)} for r in c.fetchall()])
        
    def load_all(self, path = "d:/Downloads/EDDB_v5"):
        "Import data from EDDB"
        # Outdated option: use datetime(?, 'unixepoch') on timestamp columns. (Chose to keep unix timestamp integers.)
        # TODO: support downloading with requests, progress reports, etag caching,
        # etc etc
        # SQLite performance optimization:
        c = self.cursor
        t1 = datetime.datetime.now()
        c.execute("pragma temp_store = memory")
        c.execute("pragma journal_mode = memory")
        c.execute("pragma synchronous = off")      # don't sync with every change
        c.execute("pragma foreign_keys = false")   # disable key checking during import
        # SQLite can't handle the foreign keys in this schema. 
        # Presumably it breaks because system and faction tables have foreign keys to each other,
        # even though both fields (faction home and controlling faction) may be null. 
        # Our data should be consistent, since I taught themroc to do the export in one transaction
        # TODO: Streaming downloads, progress export for GUI, missing data. 
        import_systems_populated = True
        import_systems = True
        self.disable_indices()
        def eddb_file(name):
            # Due to lack of encoding support in open, Progress does that
            return Progress(open(os.path.join(path, name), "rt"))
        if import_systems or import_systems_populated:
            #c.execute("drop index if exists eddb_systems_name_idx")
            #c.execute("drop index if exists eddb_systems_pos_idx")
            t1, t0 = datetime.datetime.now(), t1 ; print t1-t0  # 0sec
            if import_systems_populated:
                # Time: about one minute on SSD.
                # Two sources for systems: populated or all.
                # Since faction presence/influence data is only in populated, neither is a superset of the other. 
                self.load_systems_populated(eddb_file("systems_populated.jsonl"))
                self.conn.commit()
            t1, t0 = datetime.datetime.now(), t1 ; print t1-t0  # 2sec
            if import_systems:
                # Time: 
                # The unpopulated systems list is very large and takes hours to import.
                self.load_systems          (eddb_file("systems.csv"))
                self.conn.commit()
            t1, t0 = datetime.datetime.now(), t1 ; print t1-t0  # 12min20sec
            c.execute("create index if not exists eddb_systems_name_idx on eddb_systems(name)")
            c.execute("create index if not exists eddb_systems_pos_idx on eddb_systems(x,z,y,is_populated,power_id)")
            self.conn.commit()
        self.enable_indices("eddb_systems")
        t1, t0 = datetime.datetime.now(), t1 ; print t1-t0  # 0sec (already had indices?)
        # Bodies is absolutely huge and most of the data isn't helpful for traders.
        # Some of it is helpful when searching for materials, for miners and engineering. 
        ##self.load_bodies          (eddb_file("bodies.jsonl"))
        # Commodities is small and required for trade data. Sets up categories and commodities.
        self.load_commodities      (eddb_file("commodities.json"))
        self.conn.commit()
        # Modules is useful when searching for outfitting availability, not otherwise. Skip for now.
        if True:
            t1, t0 = datetime.datetime.now(), t1 ; print t1-t0  # 35 sec
            # Factions contains more information on minor factions (such as names of non-controlling)
            self.load_factions         (eddb_file("factions.csv"))
            self.conn.commit()
        if True:
            t1, t0 = datetime.datetime.now(), t1 ; print t1-t0  # 1.3 sec
            # Stations (assets in AOoS parlance) are the points where trade can occur. Load. 
            self.load_stations         (eddb_file("stations.jsonl"))
            self.conn.commit()
        if True:
            t1, t0 = datetime.datetime.now(), t1 ; print t1-t0  # 7.4 sec
            # TODO: listings.csv which links assets and commodities together. 
            # Without it, the economy types just give a rough outline of what sells where. 
            self.load_listings         (eddb_file("listings.csv"))
            self.conn.commit()
        t1, t0 = datetime.datetime.now(), t1 ; print t1-t0      # 35 sec
        self.conn.commit()
        t1, t0 = datetime.datetime.now(), t1 ; print t1-t0      # 0 sec
        #print "Enabling foreign key checks...",
        sys.stdout.flush()
        print "Enabling journal...",
        sys.stdout.flush()
        c.execute("pragma temp_store = default")    # Ran out of memory during vacuum
        c.execute("pragma journal_mode = WAL")
        #c.execute("pragma foreign_keys = true")
        c.execute("pragma synchronous = normal")
        self.conn.commit()
        print "enabling indices...",
        sys.stdout.flush()
        self.enable_indices()
        print "vacuuming...",
        sys.stdout.flush()
        c.execute("vacuum")
        self.conn.commit()
        print "analyzing...",
        sys.stdout.flush()
        c.execute("analyze")
        self.conn.commit()
        print "done."
        t1, t0 = datetime.datetime.now(), t1 ; print t1-t0
    
    def load_listings(self, file):
        "Load market data from listings.csv"
        print "Loading market data"
        # Could optimize further by arranging our SQL query columns instead 
        # of reading out of dicts for every row. 
        # The listings file is small. I should be loading the whole thing,
        # possibly sorting it, then feeding into SQLite. 
        # Or do the temporary table dance. 
        md = iter(csv.DictReader(file))
        # Ignoring the id column because we're using a composite key
        columns = "station_id commodity_id supply buy_price sell_price demand collected_at".split()
        #for index in indices.iterkeys():
        #    self.cursor.execute("drop index if exists {}".format(index))
        # FIXME: Why is the table empty after import?
        while True:
            # TODO: Drop market data if older for specific markets
            # Later todo: distinguish eddb listings from locally updated ones
            batch = list(itertools.islice(md, 1000))
            if not batch:
                break
            self.cursor.executemany("""insert or replace into eddb_market_listings
                                    (asset_id,commodity_id,supply,buy_price,sell_price,demand,collected_at)
                                    values (?, ?,           ?,     ?,        ?,         ?,     ?)"""
                                    .replace("?","nullif(?,'')"),
                            (map(l.get,columns) for l in batch))
        #for name,content in indices.iteritems():
        #    self.cursor.execute("create index if not exists {} on {}".format(name,content))
        print
    
    def load_systems(self, file):
        # Appears to function, but takes a *very* long time
        # Most recent run test: 700MB RAM (since I moved the temp storage), 2.5h
        # FIXME: A check in the database lists no unpopulated systems!
        "Load system and faction data from systems.csv"
        print "Loading systems"
        c = self.cursor
        csvr = csv.reader(file)
        sys_cols = """id,edsm_id,name,x,y,z,population,is_populated,government_id,government,allegiance_id,allegiance,state_id,state,security_id,security,primary_economy_id,primary_economy,power,power_state,power_state_id,needs_permit,updated_at,simbad_ref,controlling_minor_faction_id,controlling_minor_faction,reserve_type_id,reserve_type""".split(',')
        coldefs = [(colname, "integer primary key") if colname=="id" else
                   (colname, "integer") if colname.endswith('_id') else
                   (colname, "double") if colname in {"x","y","z"} else
                   (colname, "boolean") if colname in {"needs_permit","is_populated"} else
                   (colname, "integer") if colname=="updated_at" else  # was datetime
                   (colname, "text")
                   for colname in sys_cols]
        conv = CSVtoSQL(csvr, coldefs)
        conv.createtable(c, "systems_import")
        removed_cols = set("""government government_id 
                            allegiance allegiance_id 
                            state state_id edsm_id simbad_ref
                            security primary_economy power power_state reserve_type
                            controlling_minor_faction""".split())
        kept_cols = [col for col in sys_cols if col not in removed_cols]
        # Used 1.7GB of memory. Chunk it. 
        while True:
            # After chunking, it's clear this import runs slower and slower.
            # Are the systems indices active?
            # print "Loading CSV into temporary table"
            # sys.stdout.flush()
            batch = list(itertools.islice(csvr,50000))
            if not batch:
                break
            conv.insertinto(c, "systems_import", batch)
            if c.rowcount<=0:   # Note: does not produce 0 when ended
                # We're inserting into a temporary table, so all rows should succeed
                break
                
            self.translate_systems_batch(kept_cols, False)
            self.conn.commit()
        # The commit takes a long time. Should we do that per chunk too?
        self.conn.commit()
        c.execute('drop table systems_import')
        print
        
    def load_commodities(self, file):
        "Load commodity data from commodities.json"
        data=json.load(file)
        c=self.cursor
        c.executemany("""insert or replace into eddb_commodity_categories(id,name) values(?,?)""",
                    ((com['category']['id'],com['category']['name']) for com in data))
        c.executemany("""insert or replace into eddb_commodities
                        (id,name,category_id,average_price,is_rare) values(?,?,?,?,?)""",
                    ((com['id'],com['name'],com['category']['id'],com['average_price'],com['is_rare']) 
                     for com in data))
        

    def load_bodies(self, file):
        "Load body data from bodies.jsonl"
        raise NotImplementedError("Bodies are not yet implemented")
        print "Loading bodies"
        c = self.cursor
        # The class data is of unknown format and usefulness
        c.execute("""create temporary table bodies_import (
                        id integer, created_at integer, updated_at integer, name text,
                        system_id integer, group_id integer, group_name text, type_id integer,
                        type_name text, distance_to_arrival integer, 

                        full_spectral_class text, spectral_class text, spectral_sub_class text,
                        luminosity_class text, luminosity_sub_class text, 

                        surface_temperature real, is_main_star boolean, age double, 
                        solar_masses real, solar_radius real, catalogue_gliese_id text, 
                        catalogue_hipp_id text, catalogue_hd_id text, 
                        
                        volcanism_type_id integer, volcanism_type_name text,
                        atmosphere_type_id integer, atmosphere_type_name text,
                        terraforming_state_id integer, terraforming_state_name text,
                        
                        earth_masses real, radius real, gravity real, surface_pressure real,
                        orbital_period real, semi_major_axis real, orbital_eccentricity real,
                        orbital_inclination real, arg_of_periapsis real, rotational_period real,
                        is_rotational_period_tidally_locked boolean, axis_tilt real,
                        eg_id integer, 
                        belt_moon_masses real, 
                        ring_type_id integer, ring_type_name text,
                        ring_mass real, ring_inner_radius real, ring_outer_radius real,
                        
                        is_landable boolean)""")
        c.execute("""create temporary table bodies_import_rings (id integer, body_id integer, created_at integer,
                        updated_at integer, name text, semi_major_axis real, ring_type_id integer,
                        ring_mass real, ring_inner_radius real, ring_outer_radius real, ring_type_name text)""")
        c.execute("""create temporary table bodies_import_atmosphere_composition (body_id integer, 
                        atmosphere_component_id integer, share real, atmosphere_component_name text)""")
        c.execute("""create temporary table bodies_import_solid_composition (body_id integer, 
                        solid_component_id integer, solid_component_name text, share real)""")
        c.execute("""create temporary table bodies_import_materials (
                        body_id integer, material_id integer, material_name text, share real)""")
        # TODO: Load data and translate to database structures
        c.execute('drop table bodies_import_rings')
        c.execute('drop table bodies_import_atmosphere_composition')
        c.execute('drop table bodies_import_solid_composition')
        c.execute('drop table bodies_import_materials')
        c.execute('drop table bodies_import')
        print
    
    def load_factions(self, file):
        "Load faction data from factions.csv"
        print "Loading factions"
        c = self.cursor
        dr = iter(csv.DictReader(file))
        c.execute("""create temporary table minor_factions_import (
                        id integer, name text, updated_at integer,
                        government_id integer, government text, 
                        allegiance_id integer, allegiance text, 
                        state_id integer, state text,
                        home_system_id integer, is_player_faction boolean)""")
        fact_cols = ['id', 'name', 'updated_at', 'government_id', 'government', 'allegiance_id', 
                'allegiance', 'state_id', 'state', 'home_system_id', 'is_player_faction']
        removed_cols = set("government allegiance state".split())
        kept_cols = ','.join(col for col in fact_cols if col not in removed_cols)
        # TODO: use CSV reader class, reordering the SQL insert instead of using dictreader
        while True:
            batch = list(itertools.islice(dr, 5000))
            if not batch:
                break
            c.executemany("""insert or replace into minor_factions_import ({}) values ({})""".format(
                            ','.join(fact_cols), ','.join("nullif(?,'')"
                                                            for col in fact_cols)),
                            (map(b.get,fact_cols) for b in batch))
            #if c.rowcount<=0:   # Note: does not produce 0 when ended
            #    break
            c.execute('''insert or replace into eddb_minor_faction_states(id,name)
                         select distinct state_id,state from minor_factions_import
                         where state_id is not null''')
            c.execute('''insert or replace into eddb_governments(id, name)
                         select distinct government_id,government from minor_factions_import
                         where government_id is not null''')
            c.execute('''insert or replace into eddb_major_factions(id, name)
                         select distinct allegiance_id,allegiance from minor_factions_import
                         where allegiance_id is not null''')
            c.execute('''insert or replace into eddb_minor_factions({}) 
                         select {} from minor_factions_import'''.format(kept_cols,kept_cols))
            c.execute('delete from minor_factions_import')     # Finished with this batch
        c.execute('drop table minor_factions_import')
        print
    
    def load_stations(self, file):
        "Load asset data from stations.jsonl"
        # Takes quite a lot of time. 
        # Prerequisites (array entries): commodities, economies, ships, modules
        # Other foreign keys include system, government, allegiance, state, asset type, securities, factions, bodies
        print "Loading stations"
        c = self.cursor
        i = iter(file)
        kept_cols="""id name system_id updated_at market_updated_at distance_to_star controlling_minor_faction_id
                    type_id max_landing_pad_size is_planetary has_blackmarket has_commodities
                    has_market has_refuel has_repair has_rearm has_outfitting has_shipyard has_docking""".split()
        while True:
            batch = list(map(json.loads, itertools.islice(i, 5000)))
            if not batch:
                break
            # TODO: This version is very bad at batching. 
            # The temporary table set reduction for types is much faster. 
            # Type fillin; asset types (Coriolis etc)
            c.executemany("""insert or replace into eddb_asset_types
                    (id,name,max_landing_pad_size,is_planetary) values(?,?,?,?)""",
                    ((a['type_id'],a['type'],a['max_landing_pad_size'],a['is_planetary'])
                     for a in batch if a['type_id'] is not None))
            # Main asset table
            # TODO: join with systems for xyzindex?
            c.executemany("""insert or replace into eddb_assets({},xyzindex) 
                             select {},xyzindex from eddb_systems
                             where eddb_systems.id=?""".format
                    (','.join(kept_cols), ','.join("?" for c in kept_cols)),
                    (map(a.get,kept_cols+["system_id"]) for a in batch))
            # Many to many relations
            c.executemany("""delete from eddb_asset_imports where asset_id=?""",
                    ((a['id'],) for a in batch))
            c.executemany("""insert into eddb_asset_imports(commodity_id,asset_id)
                            select id,? from eddb_commodities where name=?""",
                    ((a['id'],c) for a in batch for c in a['import_commodities']))
            c.executemany("""delete from eddb_asset_exports where asset_id=?""",
                    ((a['id'],) for a in batch))
            c.executemany("""insert into eddb_asset_exports(commodity_id,asset_id)
                            select id,? from eddb_commodities where name=?""",
                    ((a['id'],c) for a in batch for c in a['export_commodities']))
            c.executemany("""delete from eddb_asset_prohibited where asset_id=?""",
                    ((a['id'],) for a in batch))
            c.executemany("""insert into eddb_asset_prohibited(commodity_id,asset_id)
                            select id,? from eddb_commodities where name=?""",
                    ((a['id'],c) for a in batch for c in a['prohibited_commodities']))
            # EDDB dump schema weakness: Stations don't list economy IDs.
            # We therefore only have IDs for economies that are primary for systems.
            c.executemany("""delete from eddb_asset_economies where asset_id=?""",
                    ((a['id'],) for a in batch))
            c.executemany("""insert into eddb_asset_economies(economy_id,asset_id)
                            select id,? from eddb_economies where name=?""",
                    ((a['id'],e) for a in batch for e in a['economies']))
        print
        sample = """    {"id":5,
 "name":"Reilly Hub", "system_id":396, "updated_at":1496076801, "max_landing_pad_size":"L",
 "distance_to_star":171, "government_id":64, "government":"Corporate", "allegiance_id":3,
 "allegiance":"Federation", "state_id":80, "state":"None", "type_id":8, "type":"Orbis Starport",
 "has_blackmarket":false, "has_market":true, "has_refuel":true, "has_repair":true, "has_rearm":true,
 "has_outfitting":true, "has_shipyard":true, "has_docking":true, "has_commodities":true,
 "import_commodities":["Pesticides", "Aquaponic Systems", "Biowaste"],
 "export_commodities":["Mineral Oil", "Fruit and Vegetables", "Grain"],
 "prohibited_commodities":["Narcotics", "Tobacco", "Combat Stabilisers", "Imperial Slaves", 
                           "Slaves", "Personal Weapons", "Battle Weapons", "Toxic Waste", 
                           "Wuthielo Ku Froth", "Bootleg Liquor", "Landmines"],
 "economies":["Agriculture"],
 "shipyard_updated_at":1496948943, "outfitting_updated_at":1497079599, "market_updated_at":1497079599,
 "is_planetary":false,
 "selling_ships":["Adder", "Eagle Mk. II", "Hauler", "Sidewinder Mk. I", "Viper Mk III"],
 "selling_modules":[738,739,740,743,744,745,748,749,750,753,754,755,756,757,758,759,760,761,762,
                    828,829,831,837,840,846,850,851,876,877,878,879,880,882,883,884,885,886,888,
                    891,892,893,894,896,897,898,899,929,930,934,935,937,938,941,942,943,946,947,
                    948,961,962,963,965,966,967,968,969,970,1000,1005,1009,1010,1011,1012,1013,
                    1016,1017,1018,1021,1022,1027,1032,1036,1037,1038,1041,1042,1043,1046,1047,
                    1048,1066,1067,1071,1072,1116,1119,1120,1123,1124,1125,1128,1133,1137,1138,
                    1182,1186,1191,1192,1193,1194,1195,1196,1199,1200,1201,1202,1203,1204,1207,
                    1208,1209,1212,1213,1214,1242,1243,1245,1246,1286,1306,1307,1310,1311,1316,
                    1317,1320,1321,1324,1326,1327,1373,1375,1377,1379,1381,1421,1425,1429,1523,
                    1524,1525,1526,1527,1528,1529,1530,1531,1532,1533,1534,1535,1540,1545,1549,
                    1577,1579,1581,1583,1585,1587],
 "settlement_size_id":null, "settlement_size":null,
 "settlement_security_id":null, "settlement_security":null,
 "body_id":null, "controlling_minor_faction_id":13925}      """

    def translate_systems_batch(self, kept_cols, populated=False):
        c = self.cursor
        # Collect data on global enumerations (states, governments, powers etc)
        c.execute("""insert or replace into eddb_major_factions(id,name)
                    select distinct allegiance_id,allegiance from systems_import
                    where allegiance_id is not null""")
        c.execute("""insert or ignore into eddb_powers(name) 
                    select distinct power from systems_import 
                    where power is not null""")  # Note: no ID, thus left join below
        c.execute("""insert or replace into eddb_system_power_states(id,name)
                    select distinct power_state_id,power_state from systems_import
                    where power_state_id is not null""")
        c.execute("""insert or replace into eddb_governments(id,name)
                    select distinct government_id,government from systems_import
                    where government_id is not null""")
        c.execute("""insert or replace into eddb_economies(id,name)
                    select distinct primary_economy_id,primary_economy from systems_import
                    where primary_economy_id is not null""")
        c.execute("""insert or replace into eddb_reserve_types(id,name)
                    select distinct reserve_type_id,reserve_type from systems_import
                    where reserve_type_id is not null""")
        c.execute("""insert or replace into eddb_security_levels(id,name)
                    select distinct security_id,security from systems_import
                    where security_id is not null""")
        if populated:
            c.execute("""insert or replace into eddb_minor_faction_states(id,name)
                        select distinct state_id,state from systems_presences_import
                        where state_id is not null""")
            # Ensure minor factions exist, if only by ID
            c.execute("""insert or replace into eddb_minor_factions (id)
                         select distinct minor_faction_id
                         from systems_presences_import""")
        else:
            c.execute("""insert or replace into eddb_minor_faction_states(id,name)
                        select distinct state_id,state from systems_import
                        where state_id is not null""")
            c.execute("""insert or replace into eddb_minor_factions (id,name,allegiance_id,government_id)
                         select distinct controlling_minor_faction_id,controlling_minor_faction,
                         allegiance_id,government_id from systems_import""")
        # Store data about controlling minor faction
        c.execute("""insert or replace into eddb_minor_factions (id,name,allegiance_id,government_id)
                     select distinct controlling_minor_faction_id,controlling_minor_faction,
                      allegiance_id,government_id
                     from systems_import where controlling_minor_faction is not null""")
        # Insert main portion of system table
        # self.debug_query("select * from systems_import")
        # self.debug_query("""select systems_import.controlling_minor_faction_id, eddb_minor_factions.name
                            # from systems_import left join eddb_minor_factions
                            # on eddb_minor_factions.id=systems_import.controlling_minor_faction_id""")
        # self.debug_query("""select systems_import.security_id, eddb_security_levels.name
                            # from systems_import left join eddb_security_levels
                            # on eddb_security_levels.id=systems_import.security_id""")
        # self.debug_query("""select systems_import.primary_economy_id, eddb_economies.name
                            # from systems_import left join eddb_economies
                            # on eddb_economies.id=systems_import.primary_economy_id""")
        # self.debug_query("""select systems_import.reserve_type_id, eddb_reserve_types.name
                            # from systems_import left join eddb_reserve_types
                            # on eddb_reserve_types.id=systems_import.reserve_type_id""")
        # self.debug_query("""select systems_import.power, eddb_powers.id
                            # from systems_import left join eddb_powers
                            # on eddb_powers.name=systems_import.power""")
        # self.debug_query("""select systems_import.power_state_id, eddb_system_power_states.name
                            # from systems_import left join eddb_system_power_states
                            # on eddb_system_power_states.id=systems_import.power_state_id""")
        # self.debug_query("""select eddb_systems.* from eddb_systems,systems_import 
                            # where eddb_systems.id=systems_import.id""")
        c.execute('''insert or replace into eddb_systems ({syscols},power_id,xyzindex)
                   select {sysimpcols},eddb_powers.id,spatialindex(x,y,z) from systems_import
                   left join eddb_powers on eddb_powers.name=systems_import.power
                   '''.format(
                   syscols = ','.join(kept_cols),
                   sysimpcols = ','.join("systems_import."+col for col in kept_cols)))
        if populated:
            # Replace data on minor faction presence
            c.execute('''delete from eddb_minor_faction_presence 
                        where system_id in (select id from systems_import)''')
            #self.debug_query("select * from systems_presences_import")
            c.execute('''insert into eddb_minor_faction_presence
                        (system_id,minor_faction_id,state_id,influence)
                        select system_id,minor_faction_id,state_id,influence
                        from systems_presences_import''')
            c.execute("delete from systems_presences_import")

        # Truncate temporary tables for next batch
        c.execute("delete from systems_import")

    def load_systems_populated(self, file):
        "Load system and faction data from systems_populated.jsonl"
        print "Loading systems (populated)"
        c = self.cursor
        c.execute("""create temporary table systems_import (
                        id integer, edsm_id integer, name text, x double, y double, z double,
                        population integer, is_populated boolean, government_id integer,
                        government text, allegiance_id integer, allegiance text, 
                        primary_economy text, primary_economy_id integer,
                        power text, power_state text, power_state_id integer,
                        security text, security_id integer, 
                        needs_permit boolean, updated_at integer, simbad_ref text, 
                        controlling_minor_faction_id integer, controlling_minor_faction text,
                        reserve_type_id integer, reserve_type text)""")
        c.execute("""create temporary table systems_presences_import (
                        system_id integer, minor_faction_id integer, 
                        state_id integer, influence real, state text)""")

        sys_cols="""id edsm_id name x y z population is_populated security_id security
                    government_id government allegiance_id allegiance primary_economy 
                    power power_state power_state_id needs_permit updated_at primary_economy_id
                    simbad_ref  controlling_minor_faction_id  controlling_minor_faction 
                    reserve_type_id reserve_type""".split()
        removed_cols=set("""power power_state controlling_minor_faction reserve_type 
                            primary_economy security government government_id
                            allegiance_id allegiance edsm_id simbad_ref""".split())
        kept_cols = [col for col in sys_cols if col not in removed_cols]
        while True:
            batch = map(json.loads, itertools.islice(file,15000))
            #pprint.pprint(batch)
            if not batch:
                break
            # Inject all the simple table data into primary temp table
            # Turns out JSON has empty strings in some fields that ought to be null
            c.executemany("""insert into systems_import({}) values ({})""".format(
                ",".join(sys_cols), ",".join("?" for col in sys_cols)), 
                ([system[c] if system[c]!='' else None for c in sys_cols] for system in batch))
            # Put all the presence data in second temp table
            c.executemany("""insert into systems_presences_import
                            (system_id,minor_faction_id,state_id,influence,state)
                            values (?,?,?,?,?)""", 
                            ((system['id'],p['minor_faction_id'],
                              p['state_id'],p['influence'],p['state'])
                             for system in batch
                             for p in system['minor_faction_presences']))
            try:
                self.translate_systems_batch(kept_cols, True)
            except sqlite3.OperationalError, e:
                self.debug_query('select * from systems_presences_import limit 3')
                self.debug_query('select * from systems_import limit 3')
                raise
        c.execute('drop table systems_presences_import')
        c.execute('drop table systems_import')
        #self.conn.commit()
        print

if __name__ == '__main__':
    from sys import argv
    # The idea is we might share the database with other things.
    db = EDDB(sqlite3.connect("eddb.db"))
    db.load_all()
    