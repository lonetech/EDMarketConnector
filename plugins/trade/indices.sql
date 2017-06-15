-- Indices for EDMC trade assistant
-- Enum tables are full indexed but omitted here, too small to bother with

-- Minor faction names are always accompanied by IDs in EDDB dumps, so index can be made late
create unique index if not exists 
    eddb_minor_factions_name_idx
    on eddb_minor_factions(name);

-- EDDB system IDs are referenced in other data, so we want to look them up. 
create unique index if not exists 
    eddb_systems_id_idx
    on eddb_systems(id);
-- For name lookups
create index if not exists 
    eddb_systems_name_idx
    on eddb_systems(name);

-- Only useful to find a faction, which might help with bounties. 
create index if not exists 
    eddb_minor_faction_presence_faction_idx
    on eddb_minor_faction_presence(minor_faction_id,system_id);
-- journal entries: RedeemVoucher, Bounty

-- Could we cluster assets by system? Yes, we could. 
-- Do we want to? If we do, we should do so by index value, not system_id. 
-- That means we need to find the systems when adding assets. 
-- Also means adding the system index value as a column in the assets table.
create index if not exists 
    eddb_assets_system_idx
    on eddb_assets(system_id);
create index if not exists 
    eddb_assets_name_idx
    on eddb_assets(name);
create index if not exists 
    eddb_assets_id_idx
    on eddb_assets(id);

-- Market data queries is a potentially tricky question. 
-- We usually limit ourselves to stations within a particular range, 
-- which would be supported by xyz. Using that enlarges the listings database quite a bit. 
-- This is a long table at 3M entries. Systems is much bigger, of course. 
-- The important factors are basically if supply and demand are positive,
-- which commodity it is, and where it is (by asset). 
-- Specific supply, demand, and pricing vary too much. 
-- The natural primary key is by asset and commodity. 
-- Trade searches may benefit from temporary indices?
