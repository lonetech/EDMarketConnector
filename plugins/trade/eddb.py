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

class EDDB(object):
    __slots__ = ("conn", "c")
    def __init__(self, connection):
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
        # Import data from EDDB
        self.c.execute("pragma foreign_keys = false")
        path = "d:/Downloads/EDDB_v5"
        #self.load_systems_populated(open(os.path.join(path, "systems_populated.jsonl"), "rt"))
        self.load_factions         (open(os.path.join(path, "factions.csv"), "rt"))
        self.conn.commit()
        print "Enabling foreign key checks... ",
        self.c.execute("pragma foreign_keys = true")
        self.conn.commit()
        print "done"
    
    def load_factions(self, file):
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
        def nullfilter(d,k):
            v=d.get(k)
            return v if v!="" else None
        while True:
            progress.check()
            batch = itertools.islice(dr, 5)
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
    
    def load_systems_populated(self, file):
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
    