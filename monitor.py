import atexit
from collections import defaultdict, OrderedDict
import json
import re
import threading
from os import listdir, pardir, rename, unlink, SEEK_SET, SEEK_CUR, SEEK_END
from os.path import basename, exists, isdir, isfile, join
from platform import machine
import sys
from sys import platform
from time import gmtime, sleep, strftime

if __debug__:
    from traceback import print_exc

from config import config


def tagsymbol(tag):
    symbol = re.match('\$(.+)_name;', tag, re.I)
    if symbol:
        return symbol.group(1).lower()
    else:
        return tag.lower()


if platform=='darwin':
    from AppKit import NSWorkspace
    from Foundation import NSSearchPathForDirectoriesInDomains, NSApplicationSupportDirectory, NSUserDomainMask
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
 
elif platform=='win32':
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
    import ctypes

    CSIDL_LOCAL_APPDATA     = 0x001C
    CSIDL_PROGRAM_FILESX86  = 0x002A

    # _winreg that ships with Python 2 doesn't support unicode, so do this instead
    from ctypes.wintypes import *

    HKEY_CURRENT_USER       = 0x80000001
    HKEY_LOCAL_MACHINE      = 0x80000002
    KEY_READ                = 0x00020019
    REG_SZ    = 1

    RegOpenKeyEx = ctypes.windll.advapi32.RegOpenKeyExW
    RegOpenKeyEx.restype = LONG
    RegOpenKeyEx.argtypes = [HKEY, LPCWSTR, DWORD, DWORD, ctypes.POINTER(HKEY)]

    RegCloseKey = ctypes.windll.advapi32.RegCloseKey
    RegCloseKey.restype = LONG
    RegCloseKey.argtypes = [HKEY]

    RegQueryValueEx = ctypes.windll.advapi32.RegQueryValueExW
    RegQueryValueEx.restype = LONG
    RegQueryValueEx.argtypes = [HKEY, LPCWSTR, LPCVOID, ctypes.POINTER(DWORD), LPCVOID, ctypes.POINTER(DWORD)]

    RegEnumKeyEx = ctypes.windll.advapi32.RegEnumKeyExW
    RegEnumKeyEx.restype = LONG
    RegEnumKeyEx.argtypes = [HKEY, DWORD, LPWSTR, ctypes.POINTER(DWORD), ctypes.POINTER(DWORD), LPWSTR, ctypes.POINTER(DWORD), ctypes.POINTER(FILETIME)]

    EnumWindows            = ctypes.windll.user32.EnumWindows
    EnumWindowsProc        = ctypes.WINFUNCTYPE(BOOL, HWND, LPARAM)

    CloseHandle            = ctypes.windll.kernel32.CloseHandle

    GetWindowText          = ctypes.windll.user32.GetWindowTextW
    GetWindowText.argtypes = [HWND, LPWSTR, ctypes.c_int]
    GetWindowTextLength    = ctypes.windll.user32.GetWindowTextLengthW

    GetProcessHandleFromHwnd = ctypes.windll.oleacc.GetProcessHandleFromHwnd

else:
    # Linux's inotify doesn't work over CIFS or NFS, so poll
    FileSystemEventHandler = object	# dummy


class EDLogs(FileSystemEventHandler):

    _POLL = 1		# Polling is cheap, so do it often

    # Mostly taken from http://elite-dangerous.wikia.com/wiki/List_of_Rare_Commodities
    RARES = set([
        'cetiaepyornisegg', 'aganipperush', 'alacarakmoskinart', 'albinoquechuamammoth', 'altairianskin', 'alyabodilysoap',
        'anduligafireworks', 'anynacoffee', 'aroucaconventualsweets', 'azcancriformula42', 'bluemilk',
        'baltahsinevacuumkrill', 'bakedgreebles', 'bankiamphibiousleather', 'bastsnakegin', 'belalansrayleather',
        'borasetanipathogenetics', 'burnhambiledistillate', 'cd75catcoffee', 'centaurimegagin', 'ceremonialheiketea',
        'cetirabbits', 'chameleoncloth', 'chateaudeaegaeon', 'cherbonesbloodcrystals', 'chieridanimarinepaste',
        'coquimspongiformvictuals', 'cromsilverfesh', 'crystallinespheres', 'damnacarapaces', 'deltaphoenicispalms',
        'deuringastruffles', 'disomacorn', 'aerialedenapple', 'eleuthermals', 'eraninpearlwhisky', 'eshuumbrellas',
        'esusekucaviar', 'ethgrezeteabuds', 'fujintea', 'galactictravelguide', 'geawendancedust', 'gerasiangueuzebeer',
        'giantirukamasnails', 'giantverrix', 'gilyasignatureweapons', 'gomanyauponcoffee', 'haidneblackbrew',
        'havasupaidreamcatcher', 'helvetitjpearls', 'hip10175bushmeat', 'hip118311swarm', 'hiporganophosphates',
        'hip41181squid', 'holvaduellingblades', 'honestypills', 'hr7221wheat', 'indibourbon', 'jaquesquinentianstill',
        'jaradharrepuzzlebox', 'jarouarice', 'jotunmookah', 'kachiriginleaches', 'kamitracigars', 'kamorinhistoricweapons',
        'karetiicouture', 'karsukilocusts', 'kinagoinstruments', 'konggaale', 'korrokungpellets', 'lavianbrandy',
        'alieneggs', 'leestianeviljuice', 'livehecateseaworms', 'ltthypersweet', 'lyraeweed', 'transgeniconionhead',
        'masterchefs', 'mechucoshightea', 'medbstarlube', 'mokojingbeastfeast', 'momusbogspaniel', 'motronaexperiencejelly',
        'mukusubiichitinos', 'mulachigiantfungus', 'neritusberries', 'ngadandarifireopals', 'ngunamodernantiques',
        'njangarisaddles', 'noneuclidianexotanks', 'ochoengchillies', 'onionhead', 'onionheada', 'onionheadb', 'onionheadc',
        'onionheadd', 'onionheade', 'onionheadderivatives', 'onionheadsamples', 'ophiuchiexinoartefacts',
        'orrerianviciousbrew', 'pantaaprayersticks', 'pavoniseargrubs', 'personalgifts', 'rajukrustoves',
        'rapabaosnakeskins', 'rusanioldsmokey', 'sanumameat', 'saxonwine', 'shanscharisorchid', 'soontillrelics',
        'sothiscrystallinegold', 'tanmarktranquiltea', 'tarachtorspice', 'taurichimes', 'terramaterbloodbores',
        'thehuttonmug', 'thrutiscream', 'tiegfriessynthsilk', 'tiolcewaste2pasteunits', 'toxandjivirocide', 'advert1',
        'uszaiantreegrub', 'utgaroarmillenialeggs', 'uzumokulowgwings', 'vherculisbodyrub', 'vacuumkrill',
        'vanayequirhinofur', 'vegaslimweed', 'vidavantianlace', 'lftvoidextractcoffee', 'voidworms', 'volkhabbeedrones',
        'watersofshintara', 'wheemetewheatcakes', 'witchhaulkobebeef', 'wolf1301fesh', 'wulpahyperboresystems',
        'wuthielokufroth', 'xihecompanions', 'yasokondileaf', 'zeesszeantglue', 'buckyballbeermats',
    ])

    def __init__(self):
        FileSystemEventHandler.__init__(self)	# futureproofing - not need for current version of watchdog
        self.root = None
        self.currentdir = None		# The actual logdir that we're monitoring
        self.logfile = None
        self.observer = None
        self.observed = None		# a watchdog ObservedWatch, or None if polling
        self.thread = None
        self.event_queue = []		# For communicating journal entries back to main thread

        # On startup we might be:
        # 1) Looking at an old journal file because the game isn't running or the user has exited to the main menu.
        # 2) Looking at an empty journal (only 'Fileheader') because the user is at the main menu.
        # 3) In the middle of a 'live' game.
        # If 1 or 2 a LoadGame event will happen when the game goes live.
        # If 3 we need to inject a special 'StartUp' event since consumers won't see the LoadGame event.
        self.live = False

        # Context for journal handling
        self.version = None
        self.is_beta = False
        self.mode = None
        self.group = None
        self.cmdr = None
        self.captain = None	# On a crew
        self.role = None	# Crew role - None, FireCon, FighterCon
        self.body = None
        self.system = None
        self.station = None
        self.coordinates = None
        self.state = {}		# Initialized in Fileheader

        # Cmdr state shared with EDSM and plugins
        self.state = {
            'Cargo'        : defaultdict(int),
            'Credits'      : None,
            'Loan'         : None,
            'Raw'          : defaultdict(int),
            'Manufactured' : defaultdict(int),
            'Encoded'      : defaultdict(int),
            'PaintJob'     : None,
            'Rank'         : { 'Combat': None, 'Trade': None, 'Explore': None, 'Empire': None, 'Federation': None, 'CQC': None },
            'ShipID'       : None,
            'ShipIdent'    : None,
            'ShipName'     : None,
            'ShipType'     : None,
            'Missions'     : dict(),
        }

    def set_callback(self, name, callback):
        if name in self.callbacks:
            self.callbacks[name] = callback

    def start(self, root):
        self.root = root
        logdir = config.get('journaldir') or config.default_journal_dir
        if not logdir or not exists(logdir):
            self.stop()
            return False

        if self.currentdir and self.currentdir != logdir:
            self.stop()
        self.currentdir = logdir

        # Latest pre-existing logfile - e.g. if E:D is already running. Assumes logs sort alphabetically.
        # Do this before setting up the observer in case the journal directory has gone away
        try:
            logfiles = sorted([x for x in listdir(self.currentdir) if x.startswith('Journal.') and x.endswith('.log')])
            self.logfile = logfiles and join(self.currentdir, logfiles[-1]) or None
        except:
            self.logfile = None
            return False

        # Set up a watchog observer. This is low overhead so is left running irrespective of whether monitoring is desired.
        # File system events are unreliable/non-existent over network drives on Linux.
        # We can't easily tell whether a path points to a network drive, so assume
        # any non-standard logdir might be on a network drive and poll instead.
        polling = bool(config.get('journaldir')) and platform != 'win32'
        if not polling and not self.observer:
            self.observer = Observer()
            self.observer.daemon = True
            self.observer.start()

        if not self.observed and not polling:
            self.observed = self.observer.schedule(self, self.currentdir)

        if __debug__:
            print '%s "%s"' % (polling and 'Polling' or 'Monitoring', self.currentdir)
            print 'Start logfile "%s"' % self.logfile

        if not self.running():
            self.thread = threading.Thread(target = self.worker, name = 'Journal worker')
            self.thread.daemon = True
            self.thread.start()

        return True

    def stop(self):
        if __debug__:
            print 'Stopping monitoring'
        self.currentdir = None
        self.version = self.mode = self.group = self.cmdr = self.body = self.system = self.station = self.coordinates = None
        self.is_beta = False
        if self.observed:
            self.observed = None
            self.observer.unschedule_all()
        self.thread = None	# Orphan the worker thread - will terminate at next poll

    def close(self):
        thread = self.thread
        self.stop()
        if self.observer:
            self.observer.stop()
        if thread:
            thread.join()
        if self.observer:
            self.observer.join()
            self.observer = None

    def running(self):
        return self.thread and self.thread.is_alive()

    def add_cargo(self, symbol, count=1):
        symbol = tagsymbol(symbol)
        stock = self.state['Cargo'][symbol]
        stock += count
        if stock > 0:
            self.state['Cargo'][symbol] = stock
        else:
            del self.state['Cargo'][symbol]

    def on_created(self, event):
        # watchdog callback, e.g. client (re)started.
        if not event.is_directory and basename(event.src_path).startswith('Journal.') and basename(event.src_path).endswith('.log'):
            self.logfile = event.src_path

    def worker(self):
        # Tk isn't thread-safe in general.
        # event_generate() is the only safe way to poke the main thread from this thread:
        # https://mail.python.org/pipermail/tkinter-discuss/2013-November/003522.html

        # Seek to the end of the latest log file
        logfile = self.logfile
        if logfile:
            loghandle = open(logfile, 'r')
            for line in loghandle:
                try:
                    self.parse_entry(line)	# Some events are of interest even in the past
                except:
                    if __debug__:
                        print 'Invalid journal entry "%s"' % repr(line)
        else:
            loghandle = None

        if self.live:
            if self.game_running():
                self.event_queue.append('{ "timestamp":"%s", "event":"StartUp" }' % strftime('%Y-%m-%dT%H:%M:%SZ', gmtime()))
            else:
                self.event_queue.append(None)	# Generate null event to update the display (with possibly out-of-date info)
                self.live = False

        # Watchdog thread
        emitter = self.observed and self.observer._emitter_for_watch[self.observed]	# Note: Uses undocumented attribute

        while True:

            # Check whether new log file started, e.g. client (re)started.
            if emitter and emitter.is_alive():
                newlogfile = self.logfile	# updated by on_created watchdog callback
            else:
                # Poll
                try:
                    logfiles = sorted([x for x in listdir(self.currentdir) if x.startswith('Journal.') and x.endswith('.log')])
                    newlogfile = logfiles and join(self.currentdir, logfiles[-1]) or None
                except:
                    if __debug__: print_exc()
                    newlogfile = None

            if logfile != newlogfile:
                logfile = newlogfile
                if loghandle:
                    loghandle.close()
                if logfile:
                    loghandle = open(logfile, 'r')
                if __debug__:
                    print 'New logfile "%s"' % logfile

            if logfile:
                loghandle.seek(0, SEEK_CUR)	# reset EOF flag
                for line in loghandle:
                    self.event_queue.append(line)
                if self.event_queue:
                    self.root.event_generate('<<JournalEvent>>', when="tail")

            sleep(self._POLL)

            # Check whether we're still supposed to be running
            if threading.current_thread() != self.thread:
                return	# Terminate

    def parse_entry(self, line):
        if line is None:
            return { 'event': None }	# Fake startup event

        try:
            entry = json.loads(line, object_pairs_hook=OrderedDict)	# Preserve property order because why not?
            entry['timestamp']	# we expect this to exist
            if entry['event'] == 'Fileheader':
                self.live = False
                self.version = entry['gameversion']
                self.is_beta = 'beta' in entry['gameversion'].lower()
                self.cmdr = None
                self.mode = None
                self.group = None
                self.captain = None
                self.role = None
                self.body = None
                self.system = None
                self.station = None
                self.coordinates = None
                self.state = {
                    'Cargo'        : defaultdict(int),
                    'Credits'      : None,
                    'Loan'         : None,
                    'Raw'          : defaultdict(int),
                    'Manufactured' : defaultdict(int),
                    'Encoded'      : defaultdict(int),
                    'PaintJob'     : None,
                    'Rank'         : { 'Combat': None, 'Trade': None, 'Explore': None, 'Empire': None, 'Federation': None, 'CQC': None },
                    'ShipID'       : None,
                    'ShipIdent'    : None,
                    'ShipName'     : None,
                    'ShipType'     : None,
                    'Missions'     : dict(),
                }
            elif entry['event'] == 'LoadGame':
                self.live = True
                self.cmdr = entry['Commander']
                self.mode = entry.get('GameMode')	# 'Open', 'Solo', 'Group', or None for CQC (and Training - but no LoadGame event)
                self.group = entry.get('Group')
                self.captain = None
                self.role = None
                self.body = None
                self.system = None
                self.station = None
                self.coordinates = None
                self.state.update({
                    'Credits'      : entry['Credits'],
                    'Loan'         : entry['Loan'],
                    'Rank'         : { 'Combat': None, 'Trade': None, 'Explore': None, 'Empire': None, 'Federation': None, 'CQC': None },
                })
            elif entry['event'] == 'NewCommander':
                self.cmdr = entry['Name']
                self.group = None
            elif entry['event'] == 'SetUserShipName':
                self.state['ShipID']    = entry['ShipID']
                if 'UserShipId' in entry:	# Only present when changing the ship's ident
                    self.state['ShipIdent'] = entry['UserShipId']
                self.state['ShipName']  = entry.get('UserShipName')
                self.state['ShipType']  = entry['Ship'].lower()
            elif entry['event'] == 'ShipyardBuy':
                self.state['ShipID'] = None
                self.state['ShipIdent'] = None
                self.state['ShipName']  = None
                self.state['ShipType'] = entry['ShipType'].lower()
                self.state['PaintJob'] = None
            elif entry['event'] == 'ShipyardSwap':
                self.state['ShipID'] = entry['ShipID']
                self.state['ShipIdent'] = None
                self.state['ShipName']  = None
                self.state['ShipType'] = entry['ShipType'].lower()
                self.state['PaintJob'] = None
            elif entry['event'] == 'Loadout':	# Note: Precedes LoadGame, ShipyardNew, follows ShipyardSwap, ShipyardBuy
                self.state['ShipID'] = entry['ShipID']
                self.state['ShipIdent'] = entry['ShipIdent']
                self.state['ShipName']  = entry['ShipName']
                self.state['ShipType']  = entry['Ship'].lower()
                # Ignore other Modules since they're missing Engineer modification details
                self.state['PaintJob'] = ''
                for module in entry['Modules']:
                    if module.get('Slot') == 'PaintJob' and module.get('Item'):
                        self.state['PaintJob'] = module['Item'].lower()
            elif entry['event'] in ['ModuleBuy', 'ModuleSell'] and entry['Slot'] == 'PaintJob':
                self.state['PaintJob'] = tagsymbol(entry.get('BuyItem', ''))
            elif entry['event'] in ['Undocked']:
                self.station = None
            elif entry['event'] in ['Location', 'FSDJump', 'Docked']:
                if entry['event'] != 'Docked':
                    self.body = None
                if 'StarPos' in entry:
                    self.coordinates = tuple(entry['StarPos'])
                elif self.system != entry['StarSystem']:
                    self.coordinates = None	# Docked event doesn't include coordinates
                self.system = entry['StarSystem'] == 'ProvingGround' and 'CQC' or entry['StarSystem']
                self.station = entry.get('StationName')	# May be None
            elif entry['event'] == 'SupercruiseExit':
                self.body = entry.get('BodyType') == 'Planet' and entry.get('Body')
            elif entry['event'] == 'SupercruiseEntry':
                self.body = None
            elif entry['event'] in ['Rank', 'Promotion']:
                for k,v in entry.iteritems():
                    if k in self.state['Rank']:
                        self.state['Rank'][k] = (v,0)
            elif entry['event'] == 'Progress':
                for k,v in entry.iteritems():
                    if self.state['Rank'].get(k) is not None:
                        self.state['Rank'][k] = (self.state['Rank'][k][0], min(v, 100))	# perhaps not taken promotion mission yet

            elif entry['event'] == 'Cargo':
                self.live = True	# First event in 2.3
                self.state['Cargo'] = defaultdict(int)
                self.state['Cargo'].update({ x['Name']: x['Count'] for x in entry['Inventory'] })
            elif entry['event'] in ['CollectCargo', 'MarketBuy', 'MiningRefined', 'PowerplayCollect', 'BuyDrones']:
                self.add_cargo(entry['Type'], entry.get('Count', 1))
            elif entry['event'] in ['EjectCargo', 'MarketSell', 'PowerplayDeliver', 'SellDrones']:
                self.add_cargo(entry['Type'], -entry.get('Count', 1))
            elif entry['event'] == 'MissionAccepted':
                self.state['Missions'][entry['MissionID']] = entry
                if entry['Name'].split('_')[1:2] == ['Delivery']:
                    self.add_cargo(entry['Commodity'], entry.get('Count', 1))
            elif entry['event'] == 'MissionCompleted':
                oldmission = self.state['Missions'].pop(entry['MissionID'], None)
                missiontype = entry['Name'].split('_')
                if len(missiontype)>1 and missiontype[1] in ['Delivery', 'Collect']:
                    self.add_cargo(entry['Commodity'], -entry.get('Count', 1))
                # Not sure whether the names for 'CommodityReward' are from the same namespace as the 'Cargo' event.
                for reward in entry.get('CommodityReward', []):
                    self.add_cargo(reward['Name'].lower(), reward.get('Count', 1))

            elif entry['event'] == 'Materials':
                for category in ['Raw', 'Manufactured', 'Encoded']:
                    self.state[category] = defaultdict(int)
                    self.state[category].update({ x['Name']: x['Count'] for x in entry.get(category, []) })
            elif entry['event'] == 'MaterialCollected':
                self.state[entry['Category']][entry['Name']] += entry['Count']
            elif entry['event'] in ['MaterialDiscarded', 'ScientificResearch']:
                self.state[entry['Category']][entry['Name']] -= entry['Count']
                if self.state[entry['Category']][entry['Name']] <= 0:
                    self.state[entry['Category']].pop(entry['Name'])
            elif entry['event'] in ['EngineerCraft', 'Synthesis']:
                for category in ['Raw', 'Manufactured', 'Encoded']:
                    for x in entry[entry['event'] == 'EngineerCraft' and 'Ingredients' or 'Materials']:
                        if x['Name'] in self.state[category]:
                            self.state[category][x['Name']] -= x['Count']
                            if self.state[category][x['Name']] <= 0:
                                self.state[category].pop(x['Name'])

            elif entry['event'] == 'JoinACrew':
                self.captain = entry['Captain']
                self.role = None
                self.body = None
                self.system = None
                self.station = None
                self.coordinates = None
            elif entry['event'] == 'ChangeCrewRole':
                self.role = entry['Role'] != 'Idle' and entry['Role'] or None
            elif entry['event'] == 'QuitACrew':
                self.captain = None
                self.role = None
                self.body = None
                self.system = None
                self.station = None
                self.coordinates = None

            return entry
        except:
            if __debug__:
                print 'Invalid journal entry "%s"' % repr(line)
                print_exc()
            return { 'event': None }

    def get_entry(self):
        if not self.event_queue:
            return None
        else:
            entry = self.parse_entry(self.event_queue.pop(0))
            if not self.live and entry['event'] not in [None, 'Fileheader']:
                self.live = True
                self.event_queue.append('{ "timestamp":"%s", "event":"StartUp" }' % strftime('%Y-%m-%dT%H:%M:%SZ', gmtime()))
            return entry

    def carrying_rares(self):
        for commodity in self.state['Cargo']:
            if commodity in self.RARES:
                return True
        return False

    def game_running(self):

        if platform == 'darwin':
            for app in NSWorkspace.sharedWorkspace().runningApplications():
                if app.bundleIdentifier() == 'uk.co.frontier.EliteDangerous':
                    return True

        elif platform == 'win32':

            def WindowTitle(h):
                if h:
                    l = GetWindowTextLength(h) + 1
                    buf = ctypes.create_unicode_buffer(l)
                    if GetWindowText(h, buf, l):
                        return buf.value
                return None

            def callback(hWnd, lParam):
                name = WindowTitle(hWnd)
                if name and name.startswith('Elite - Dangerous'):
                    handle = GetProcessHandleFromHwnd(hWnd)
                    if handle:	# If GetProcessHandleFromHwnd succeeds then the app is already running as this user
                        CloseHandle(handle)
                        return False	# stop enumeration
                return True

            return not EnumWindows(EnumWindowsProc(callback), 0)

        return False


# singleton
monitor = EDLogs()
