# Python 2 code to fetch data from EDDB in a reasonably friendly manner

#import zlib, urllib2, json, csv

# ... EDMC includes Requests. Use it. 
# It would be possible to use urllib, decode via zlib, etc. 
# Requests does it by default and on the fly. 
# Only etag caching remains. 
# Add an "If-None-Match" header with the received ETag value.
# Check result code for 304 Not Modified. 


import sqlite3, os, json, csv, sys, itertools
import pprint

class Progress(object):
    __slots__ = ("end", "file")
    def __init__(self, file):
        self.file = file
        file.seek(0, os.SEEK_END)
        self.end = file.tell()
        file.seek(0, os.SEEK_SET)
    def check(self):
        pos = self.file.tell()
        sys.stdout.write("\r{}/{}".format(pos,self.end))
        sys.stdout.flush()
        return pos<self.end

def nullfilter(d,k):
    "Detects nulls (empty columns) and decodes unicode text"
    v=d.get(k)
    if isinstance(v,str):
        return v.decode('utf-8') if v else None
    return v

class EDDB(object):
    __slots__ = ("conn", "c")
    def __init__(self, connection):
        # Given a database connection, ensures EDDB data schema is in there.
        self.conn = connection
        self.c = connection.cursor()
        # Ensure we have the appropriate tables
        table_defs = open(os.path.join(os.path.dirname(__file__), "tables.sql")).read()
        # This silly loop can be replaced with executescript. Oh well. 
        for table_def in table_defs.split(';'):
            try:
                self.c.execute(table_def)
            except:
                print table_def
                raise
        self.conn.commit()
        try:
            self.c.execute('''alter table eddb_minor_factions 
                           add home_system_id integer references eddb_systems(id)''')
            self.conn.commit()
        except sqlite3.OperationalError, e:
            if e.message != "duplicate column name: home_system_id":
                raise
    
    def __del__(self):
        #self.c.execute("pragma optimize")
        pass
    
    def load_all(self, path = "d:/Downloads/EDDB_v5"):
        "Import data from EDDB"
        # TODO: support downloading with requests, progress reports, etag caching,
        # etc etc
        # SQLite performance optimization:
        self.c.execute("pragma journal_mode = WAL")     # write ahead logging
        self.c.execute("pragma synchronous = off")      # don't sync with every change
        self.c.execute("pragma foreign_keys = false")   # disable key checking during import
        # Our data should be consistent, since I taught themroc to do the export in one transaction
        # TODO: Streaming downloads, progress export for GUI, missing data. 
        if False:
            # Two sources for systems: populated or all.
            # Since faction data is only in populated, neither is a superset of the other. 
            self.load_systems_populated(open(os.path.join(path, "systems_populated.jsonl"), "rt"))
            # The unpopulated systems list is very large and takes hours to import.
            self.load_systems          (open(os.path.join(path, "systems.csv"), "rt"))
        # Bodies is absolutely huge and most of the data isn't helpful for traders.
        # Some of it is helpful when searching for materials, for miners and engineering. 
        ##self.load_bodies          (open(os.path.join(path, "bodies.jsonl"), "rt"))
        # Commodities is small and required for trade data. Sets up categories and commodities.
        self.load_commodities      (open(os.path.join(path, "commodities.json"), "rt"))
        # Modules is useful when searching for outfitting availability, not otherwise. Skip for now.
        if False:
            # Factions contains more information on minor factions (such as names of non-controlling)
            self.load_factions         (open(os.path.join(path, "factions.csv"), "rt"))
        if True:
            # Stations (assets in AOoS parlance) are the points where trade can occur. Load. 
            self.load_stations         (open(os.path.join(path, "stations.jsonl"), "rt"))
        if True:
            # TODO: listings.csv which links assets and commodities together. 
            # Without it, the economy types just give a rough outline of what sells where. 
            pass
        self.conn.commit()
        print "Enabling foreign key checks... ",
        self.c.execute("pragma foreign_keys = true")
        self.c.execute("pragma synchronous = normal")
        self.conn.commit()
        print "vacuuming... ",
        self.c.execute("vacuum")
        self.conn.commit()
        print "analyzing... ",
        self.c.execute("analyze")
        self.conn.commit()
        print "done"
        
    def load_systems(self, file):
        # Appears to function, but takes a *very* long time
        "Load system and faction data from systems.csv"
        print "Loading systems"
        progress = Progress(file)
        c = self.c
        dr = iter(csv.DictReader(file))
        c.execute("""create temporary table systems_import (
                        id integer, name text, updated_at datetime,
                        government_id integer, government text, 
                        allegiance_id integer, allegiance text,
                        state_id integer, state text,
                        home_system_id integer, is_player_faction boolean)""")
        sys_cols = ("id,name,updated_at,government_id,government,allegiance_id,allegiance,"+
                    "state_id,state,home_system_id,is_player_faction").split(',')
        removed_cols = set("government allegiance state".split())
        kept_cols = ','.join(col for col in sys_cols if col not in removed_cols)
        while True:
            progress.check()
            batch = itertools.islice(dr, 50000)
            c.executemany("""insert into systems_import ({}) values ({})""".format(
                            ','.join(sys_cols), ','.join('?' for col in sys_cols)),
                            ([nullfilter(b,c) for c in sys_cols] for b in batch))
            if c.rowcount<=0:   # Note: does not produce 0 when ended
                break
            #c.execute('select * from systems_import')
            #pprint.pprint(c.fetchall())
            c.execute('''insert or replace into eddb_minor_faction_states(id,name)
                         select distinct state_id,state from systems_import
                         where state_id is not null''')
            c.execute('''insert or replace into eddb_governments(id, name)
                         select distinct government_id,government from systems_import
                         where government_id is not null''')
            c.execute('''insert or replace into eddb_major_factions(id, name)
                         select distinct allegiance_id,allegiance from systems_import
                         where allegiance_id is not null''')
            c.execute('''insert or replace into eddb_minor_factions({}) 
                         select {} from systems_import'''.format(kept_cols,kept_cols))
            c.execute('delete from systems_import')     # Finished with this batch
            self.conn.commit()      
            # Tried committing often to limit the log size. 
        c.execute('drop table systems_import')
        print
        
    def load_commodities(self, file):
        "Load commodity data from commodities.json"
        data=json.load(file)
        c=self.c
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
        progress = Progress(file)
        c = self.c
        # The class data is of unknown format and usefulness
        c.execute("""create temporary table bodies_import (
                        id integer, created_at datetime, updated_at datetime, name text,
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
        c.execute("""create temporary table bodies_import_rings (id integer, body_id integer, created_at datetime,
                        updated_at datetime, name text, semi_major_axis real, ring_type_id integer,
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
        progress = Progress(file)
        c = self.c
        dr = iter(csv.DictReader(file))
        c.execute("""create temporary table minor_factions_import (
                        id integer, name text, updated_at datetime,
                        government_id integer, government text, 
                        allegiance_id integer, allegiance text, 
                        state_id integer, state text,
                        home_system_id integer, is_player_faction boolean)""")
        fact_cols = ['id', 'name', 'updated_at', 'government_id', 'government', 'allegiance_id', 
                'allegiance', 'state_id', 'state', 'home_system_id', 'is_player_faction']
        removed_cols = set("government allegiance state".split())
        kept_cols = ','.join(col for col in fact_cols if col not in removed_cols)
        while True:
            progress.check()
            batch = itertools.islice(dr, 5000)
            c.executemany("""insert into minor_factions_import ({}) values ({})""".format(
                            ','.join(fact_cols), ','.join('?' for col in fact_cols)),
                            ([nullfilter(b,c) for c in fact_cols] for b in batch))
            if c.rowcount<=0:   # Note: does not produce 0 when ended
                break
            #c.execute('select * from minor_factions_import')
            #pprint.pprint(c.fetchall())
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
        progress = Progress(file)
        c = self.c
        i = iter(file)
        kept_cols="""id name system_id updated_at market_updated_at distance_to_star controlling_minor_faction_id
                    type_id max_landing_pad_size is_planetary has_blackmarket has_commodities
                    has_market has_refuel has_repair has_rearm has_outfitting has_shipyard has_docking""".split()
        while True:
            progress.check()
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
            c.executemany("""insert or replace into eddb_assets({}) values({})""".format
                    (','.join(kept_cols), ','.join('?' for c in kept_cols)),
                    ([nullfilter(a,c) for c in kept_cols] for a in batch))
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

    
    def load_systems_populated(self, file):
        "Load system and faction data from systems_populated.jsonl"
        # TODO: Progress indication
        print "Loading systems (populated)"
        progress = Progress(file)
        c = self.c
        c.execute("""create temporary table systems_import (
                        id integer, edsm_id integer, name text, x double, y double, z double,
                        population integer, is_populated boolean, government_id integer,
                        government text, allegiance_id integer, allegiance text, 
                        primary_economy text, primary_economy_id integer,
                        power text, power_state text, power_state_id integer,
                        security text, security_id integer, 
                        needs_permit boolean, updated_at datetime, simbad_ref text, 
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
                            allegiance_id allegiance""".split())
        kept_cols = [col for col in sys_cols if col not in removed_cols]
        def translate_batch():
            # Collect data on global enumerations (states, governments, powers etc)
            c.execute("""insert or replace into eddb_major_factions(id,name)
                        select distinct allegiance_id,allegiance from systems_import
                        where allegiance_id is not null""")
            c.execute("""insert or ignore into eddb_powers(name) 
                        select distinct power from systems_import 
                        where power is not null""")  # Note: no ID, thus left join below
            c.execute("""insert or replace into eddb_system_power_state(id,name)
                        select distinct power_state_id,power_state from systems_import
                        where power_state_id is not null""")
            c.execute("""insert or replace into eddb_minor_faction_states(id,name)
                        select distinct state_id,state from systems_presences_import
                        where state_id is not null""")
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
            # Ensure minor factions exist, if only by ID
            c.execute("""insert or replace into eddb_minor_factions (id)
                         select distinct minor_faction_id
                         from systems_presences_import""")
            # Store data about controlling minor faction
            c.execute("""insert or replace into eddb_minor_factions (id,name,allegiance_id,government_id)
                         select distinct controlling_minor_faction_id,controlling_minor_faction,
                          allegiance_id,government_id
                         from systems_import where controlling_minor_faction is not null""")
            # Insert main portion of system table
            c.execute('''insert or replace into eddb_systems ({},power_id)
                       select {},eddb_powers.id from systems_import
                       left join eddb_powers on power=eddb_powers.name
                       '''.format(
                       ','.join(kept_cols),
                       ','.join("systems_import."+col for col in kept_cols)))
            # Replace data on minor faction presence
            c.execute('''delete from eddb_minor_faction_presence 
                        where system_id in (select system_id from systems_presences_import)''')
            c.execute('''insert into eddb_minor_faction_presence
                        (system_id,minor_faction_id,state_id,influence)
                        select system_id,minor_faction_id,state_id,influence
                        from systems_presences_import''')

            # Truncate temporary tables for next batch
            c.execute("delete from systems_import")
            c.execute("delete from systems_presences_import")
        for line in file:
            system = json.loads(line)
            # Inject all the simple table data into primary temp table
            c.execute("""insert into systems_import({}) values ({})""".format(
                ",".join(sys_cols), ",".join("?" for col in sys_cols)), 
                map(system.get, sys_cols))
            # Put all the presence data in second temp table
            #pprint.pprint(system['minor_faction_presences'])
            c.executemany("""insert into systems_presences_import
                            (system_id,minor_faction_id,state_id,influence,state)
                            values (?,?,?,?,?)""", 
                            [(system['id'],p['minor_faction_id'],
                              p['state_id'],p['influence'],p['state'])
                              for p in system['minor_faction_presences']])
            lines+=1
            
            if lines>=15000:    # TODO: Translate to batched inserts above?
                progress.check()
                try:
                    translate_batch()
                except sqlite3.OperationalError, e:
                    c.execute('select * from systems_presences_import')
                    pprint.pprint(c.fetchall())
                    c.execute('select * from systems_import')
                    pprint.pprint(c.fetchall())
                    raise
        if lines:
            translate_batch()
        c.execute('drop table systems_presences_import')
        c.execute('drop table systems_import')
        #self.conn.commit()
        print

if __name__ == '__main__':
    from sys import argv
    # The idea is we might share the database with other things.
    db = EDDB(sqlite3.connect("eddb.db"))
    db.load_all()
    