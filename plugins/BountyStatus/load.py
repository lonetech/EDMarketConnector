# EDMC plugin to show current bounty / combat bonds status

try:
    from Tkinter import ttk
except ImportError:
    import ttk
import Tkinter as tk
import myNotebook as nb
from config import config

from collections import defaultdict
import webbrowser
from operator import itemgetter
try:
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlencode

victims = defaultdict(int)
bounties = defaultdict(int)
bonds = defaultdict(int)

treeview = None
viewroot = ""

# Mahon Mission Log URL
reporturl = """https://docs.google.com/forms/d/e/1FAIpQLSdpZSlFXwmNFoK0O6o7pY24xpM9Uhx-G5WAJ22rzPlfqk_pMw/viewform"""
reportfields = {
    "entry.1329248460": "Commander",
    "entry.248483210": "Activity",  # Bounties, Combat Bonds
    "entry.1839481350": "Amount",
    "entry.1757640172": "System",
    "entry.1498245084": "Faction",
#    "entry.497422052": "Mission influence pips",
#    "entry.1112079590": "Transaction count",
}

# Identical functions, distinct sources
def update_treeview(source):
    entries = sorted(source.iteritems(), key=itemgetter(1), reverse=True)
    treeview.set_children(viewroot)
    for faction, amount in entries:
        child=treeview.insert(viewroot, "end", text=faction, values=[str(amount)])

# Functions called by EDMC
def plugin_start():
    pass

def plugin_app(parent):
    global treeview
    treeview = ttk.Treeview(parent, columns=2)
    treeview.heading("#0", text="Faction")
    treeview.heading(0, text="Credits")
    
    bounties["foo"] = 123
    update_treeview(bounties)
    #treeview.set_children("", "Recent bounties", "Recent combat bonds")
    return treeview

def journal_entry(cmdr, system, station, entry, state):
    global victims, bounties, bonds
    # Relevant events:
    # SupercruiseExit should mark when we enter a CZ, pirate zone etc
    # Can start counting from then if we like
    # Would be nice if the faction info was there
    if entry['event']==u"SupercruiseExit":
        # Reset our data
        victims = defaultdict(int)
        bounties = defaultdict(int)
        bonds = defaultdict(int)
    elif entry['event']==u"Bounty":
        # Count a bounty
        victims[entry['VictimFaction']] += 1
        for subentry in entry['Rewards']:
            bounties[subentry['Faction']] += subentry['Reward']
        update_treeview(bounties)
    elif entry['event']==u"FactionKillBond":
        bonds[entry['AwardingFaction']] += entry['Reward']
        update_treeview(bonds)
    elif entry['event'] == u"RedeemVoucher":
        # BGS report portion
        if entry['Type'] == u'bounty':
            activity = "Bounties"
            # Turned in a bounty voucher. Fire up a report.
            for f in entry['Factions']:
                d = {"Commander": cmdr, "System": system, "Activity": activity}
                d.update(f)
                fields = {key: d[field] for (key, field) in reportfields.items()
                            if field in d}
                url="{}?{}".format(reporturl, urlencode(fields))
                webbrowser.open(url)
        elif entry['Type'] == u'CombatBond':
            activity = "Combat Bonds"
            # Turned in a bounty voucher. Fire up a report.
            d = {"Commander": cmdr, "System": system, "Activity": activity}
            d.update(entry)
            fields = {key: d[field] for (key, field) in reportfields.items()
                        if field in d}
            url="{}?{}".format(reporturl, urlencode(fields))
            webbrowser.open(url)
