# EDMarketConnector plugin to examine the available data

try:
	from Tkinter import ttk
except ImportError:
	import ttk
import Tkinter as tk
import myNotebook as nb
from config import config

import pprint

# Updated by update function
data = {'entries':[]}		# printable data
view = None			# view widget

# Functions called by EDMC
def plugin_start():
	pass

def plugin_app(parent):
	button = ttk.Button(parent, text="Show data", command=show_data)
	return button

def journal_entry(cmdr, system, station, entry, state):
	# This is where real flexibility lives. 
	# Every entry is the decoded JSON line from the journal. 
	# This ranges from useless (NPC chatter) to really interesting. 
	data['cmdr'] = cmdr
	data['system'] = system
	data['station'] = station
	#data['entry'] = entry
	data['entries'].append(entry)
	data['state'] = state
	update_data(data)
	# Example entry of interest: PowerplayCollect
	# Documentation at https://forums.frontier.co.uk/showthread.php/275151-Commanders-log-manual-and-data-sample

def cmdr_data(data_):
	# data contains market data etc, decoded from EDAPI.
	# For instance, data['lastStarport'] is the last place we docked,
	# including market data (commodities) and modules (modules). 
	# It has an id but it doesn't match EDDB. 
	# Combine with 'lastSystem' name for lookups. 
	data['data'] = data_
	update_data(data)

# Functions to handle our UI

# Tree view displayer for structured data
def datatree(data):
	global view
	def enter_children(parent, data):
		if isinstance(data, dict):
			for key,value in data.items():
				child = view.insert(parent, "end", text=key)
				# TODO set value
				enter_children(child, value)
		elif isinstance(data, list):
			for index,value in enumerate(data):
				child = view.insert(parent, "end", text=pprint.pformat(index))
				enter_children(child, value)
		else:
			view.item(parent, values=[pprint.pformat(data)])
	view.set_children("")		# empty tree
	enter_children("", data)

def show_data():
	w = tk.Toplevel()
	w.title("EDMC Data")
	global view
	#text = tk.Text(w)
	view = ttk.Treeview(w, columns=1)
	update_data(data)
	view.pack(fill=tk.BOTH, expand=1)

def update_data(data):
	global view
	if view:
	#	text.delete("0.0")
	#	text.insert("0.0", pprint.pformat(data))
		datatree(data)
