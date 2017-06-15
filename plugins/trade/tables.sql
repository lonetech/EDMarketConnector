-- SQLite database schema for EDDB v5 data

-- TODO Decide whether to split in distinct databases and use schema-name

-- Split into data and constraints.
-- Need to drop and recreate indices while loading dumps.

-- We can rely on EDDB updates to satisfy foreign key constraints,
-- which means we don't need any of the reverse lookup indices they create. 
--pragma foreign_keys = true;

-- datetime is inefficient in SQLite, so keep the unix timestamp format
-- applies to updated_at, created_at, collected_at columns

-- The enum tables are fully indexed backwards and forwards, but fairly small

-- Faction and system tables
create table if not exists eddb_major_factions (
    id integer primary key,    -- eddb_systems.allegiance_id
    name text unique not null  -- eddb_systems.allegiance
);

-- Supposedly global. Is a faction at war if the war is in another system?
create table if not exists eddb_minor_faction_states (
    id integer primary key,    -- eddb_systems.minor_faction_presences[i].state_id
    name text unique not null  -- eddb_systems.minor_faction_presences[i].state
);

create table if not exists eddb_governments (
    id integer primary key,    -- eddb_systems.government_id
    name text unique not null  -- eddb_systems.government
);

create table if not exists eddb_security_levels (
    id integer primary key,
    name text unique not null
);

create table if not exists eddb_powers (
    id integer primary key,
    name text unique not null
);

create table if not exists eddb_system_power_states (
    id integer primary key,
    name text unique not null
);

create table if not exists eddb_economies (
    id integer primary key,
    name text unique not null
);

create table if not exists eddb_reserve_types (
    id integer primary key,
    name text unique not null
);

-- Index needs: lookups by ID are frequent, name rarer.
-- Name lookups are basically manual search. 
create table if not exists eddb_minor_factions (
-- from CSV:
--id,name,updated_at,government_id,government,allegiance_id,allegiance,state_id,state,home_system_id,is_player_faction
    id integer primary key,    -- eddb_systems.minor_faction_presences[i].minor_faction_id
    name text, -- unique not null,   -- systems refer to factions before we know them. factions refer to systems. 
    allegiance_id integer, -- references eddb_major_factions(id),
    government_id integer, -- references eddb_governments(id),
    state_id integer, -- references eddb_minor_faction_states(id),
    updated_at integer,   -- may help with tracking state. do we need that?
    -- This column was treated specially because the foreign keys go both ways between these tables.
    -- Since I don't do foreign keys now, I don't need it. 
    home_system_id integer, -- references eddb_systems(id),   -- Created later, there's no forward declaration
    is_player_faction boolean
);
    

-- Index needs:
-- ID lookup is used when going from station, and basically not otherwise.
-- Name lookup is frequent (find current location). 
-- Spatial locality is relevant, when searching xyz. 
-- Populated systems are the only relevant ones for station lookups. 
-- EDDB IDs are not in a particularly helpful order, nor are EDSM IDs, which may not be present.
-- Frontier's IDs are probably not to be trusted. 
-- Idea: Create a spatial index order with S-curve (mingle bits from XYZ). 
-- Cluster systems by using a composite key on that,xyz,is_populated,system_id, without rowid.

-- Spatial index algorithm: First, define a useful range. 
-- The Milky Way is about 100kly in diameter, so let's make our limit 200kly. 
-- Database ranges are within 100kly. We probably can't reach outliers that don't fit. 
-- This should require no more than 18 bits for ly precision (good enough for index).
-- We can write the index mapping with Python and hook it up with create_function. 
-- Doing so means our primary spatial index won't help manual lookups (like sqlite cli).

create table if not exists eddb_systems (
-- from CSV:
--id,edsm_id,name,x,y,z,population,is_populated,
--government_id,government,allegiance_id,allegiance,state_id,state,controlling_minor_faction_id,controlling_minor_faction,
--security_id,security,primary_economy_id,primary_economy,power,power_state,power_state_id,
--needs_permit,updated_at,simbad_ref,reserve_type_id,reserve_type

    id integer, -- unique not null,
    --edsm_id integer unique,
    --simbad_ref text unique,
    
    name text,
    population integer,
    is_populated boolean,
    
    -- Fields that are actually from controlling minor faction
    controlling_minor_faction_id integer, -- references eddb_minor_factions(id),
    --allegiance_id integer references eddb_major_factions(id),
    --government_id integer references eddb_governments(id),
    --state_id integer references eddb_minor_faction_states(id),   -- in eddb_minor_faction_presence
    -- Fields that are faction related but may be system specific
    security_id integer, -- references eddb_security_levels(id),
    
    -- Fields that come from controlling asset
    primary_economy_id integer, -- references eddb_economies(id),
    
    reserve_type_id integer, -- references eddb_reserve_types(id),
    
    -- Power play related
    power_id integer, -- references eddb_powers(id),
    power_state_id integer, -- references eddb_system_power_states(id),
    
    needs_permit boolean,

    updated_at integer, -- unix timestamp    
    
    x double,
    y double,
    z double,
    xyzindex integer,    -- will usually be 8 bytes

    -- Here's our primary key, actually reordering the table itself
    -- Usually the first column suffices, and y has the smallest range
    constraint eddb_systems_spatial_idx
    primary key (xyzindex, is_populated, id)
) without rowid;    -- Permits the primary key to cluster data


-- Funny little table. Looked up by system, possibly faction. 
-- Should be composite index.
create table if not exists eddb_minor_faction_presence (
    minor_faction_id integer, -- references eddb_minor_factions(id),
    system_id integer, -- references eddb_systems(id),
    state_id integer, -- references eddb_minor_faction_states(id),
    influence real,
    constraint eddb_minor_faction_presence_key primary key(system_id,minor_faction_id)
) without rowid;

-- Body tables: Not yet done.

-- Commodity tables
create table if not exists eddb_commodity_categories (
    id integer primary key, 
    name text unique not null);

create table if not exists eddb_commodities (
    id integer primary key,
    name text unique not null,
    category_id integer, -- references eddb_commodity_categories(id),
    average_price integer,
    is_rare boolean
);

-- Asset tables (data from stations.jsonl)
create table if not exists eddb_asset_types (
    id integer primary key, 
    name text unique not null,
    max_landing_pad_size text,
    is_planetary boolean);

create table if not exists eddb_assets (
    id integer, -- primary key,
    name text not null,
    system_id integer not null references eddb_systems(id),
    
    --timestamps
    updated_at integer,
    --shipyard_updated_at integer,
    --outfitting_updated_at integer,
    market_updated_at integer,

    -- Body data, but there needn't be a body.
    --body_id
    distance_to_star integer,

    controlling_minor_faction_id integer, -- references eddb_minor_factions(id),
    -- These are actually faction data. Might still be useful for indices?
    --government_id integer references eddb_governments(id),
    --allegiance_id integer references eddb_major_factions(id),
    --state_id integer references eddb_minor_faction_states(id),  -- in system presence

    -- Asset type data. only ID is required, but the others may help indexing.
    type_id integer, -- references eddb_asset_types(id),
    max_landing_pad_size text,  -- Single character. Values S,M,L.
    is_planetary boolean,
    --settlement_size_id
    --settlement_size
    --settlement_security_id
    --settlement_security
    
    has_blackmarket boolean,
    has_market boolean,
    has_refuel boolean,
    has_repair boolean,
    has_rearm boolean,
    has_outfitting boolean,
    has_shipyard boolean,
    has_docking boolean,
    has_commodities boolean,    -- Is this the same as has_market ?
    
    -- import_commodities, export_commodities, prohibited_commodities, economies,
    -- selling_ships and selling_modules are all many to many, so put in separate tables
    
    -- System's position is in here for spatial clustering of data
    xyzindex integer,

    -- Uses id for tie breaker. Clusters data spatially. 
    -- We also need lookups by system id, asset id, and name. 
    -- We would have distance_to_star in there, but there are assets with null in it!
    constraint eddb_assets_spatial_key primary key(xyzindex,id)
) without rowid;
-- This trigger does not work. Need to load xyzindex some other way. 
--create trigger if not exists eddb_asset_find_system 
--before insert on eddb_assets for each row when new.xyzindex is null
--begin update new set xyzindex=(
--    select xyzindex from eddb_systems,new 
--    where eddb_systems.system_id=new.system_id);

-- Indexing these by asset makes them easier to find.
create table if not exists eddb_asset_imports (
    commodity_id integer, -- references eddb_commodities(id) not null,
    asset_id integer, -- references eddb_assets(id) not null,
    constraint eddb_asset_imports_key primary key(asset_id,commodity_id)
) without rowid;
create table if not exists eddb_asset_exports (
    commodity_id integer, -- references eddb_commodities(id) not null,
    asset_id integer, -- references eddb_assets(id) not null
    constraint eddb_asset_exports_key primary key(asset_id,commodity_id)
) without rowid;
create table if not exists eddb_asset_prohibited (
    commodity_id integer, -- references eddb_commodities(id) not null,
    asset_id integer, -- references eddb_assets(id) not null
    constraint eddb_asset_prohibited_key primary key(asset_id,commodity_id)
) without rowid;
create table if not exists eddb_asset_economies (
    economy_id integer references eddb_economies(id) not null,
    asset_id integer references eddb_assets(id) not null,
    constraint eddb_asset_economies_key primary key(asset_id,economy_id)
) without rowid;

    
-- Market data
-- Note: Out of ID order CSV dumps may be slowing the database down during import.
-- It might actually help performance to cluster by asset_id instead of id. 
-- This table is the very easiest to import, consisting of a CSV with only integers.
create table if not exists eddb_market_listings (
    --id integer, -- primary key,
    asset_id integer, -- references eddb_assets(id) not null,
    commodity_id integer, -- references eddb_commodities(id) not null,
    supply integer,
    buy_price integer,
    sell_price integer,
    demand integer,
    collected_at integer,
    constraint eddb_market_listings_key primary key(asset_id,commodity_id)
) without rowid;
-- Will this work nicely when replacing data? I *think* the delete would leave 
-- space for new rows, so we can delete+insert instead of update. 

-- -- Indices are important this time. We tend to be looking for trade price on particular 
-- -- commodities, including asset ID to limit it to nearby. However, indexing should be delayed. 
-- Possibly add collected_at to the indices for restricted searches?
-- create index if not exists eddb_listing_byasset 
-- on eddb_market_listings(asset_id,commodity_id);
-- -- TODO: partial indices for where they don't buy or sell?
-- create index if not exists eddb_listing_sellprice
-- on eddb_market_listings(commodity_id,sell_price,asset_id);
-- create index if not exists eddb_listing_buyprice
-- on eddb_market_listings(commodity_id,buy_price,asset_id);
