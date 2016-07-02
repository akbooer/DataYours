module ("L_DataWatcher", package.seeall)

------------------------------------------------------------------------
--
-- DataWatcher mimics a Carbon "relay" daemon, 
-- a data collection front-end using luup.watch
-- relaying Luup variable changes to other Carbon daemons and/or syslog
--
-- HTTP server is http://127.0.0.1:3480/data_request?id=lr_DataWatcher
-- HTTP requests:
--  &watch=devNo.serviceId.variable     starts watching this variable, or
--  &nowatch=devNo.serviceId.variable   stops watching this variable (effective on next restart only)
--

-- 2016.02.26   use relative URL for render call (works when used remotely)
-- 2016.04.07   add user-defined processing to filter outgoing metric values
-- 2016.04.08   add LINE_RECEIVER_PORT for incoming UDP Whisper plaintext messages to relay to DESTINATIONS
--              test with Linux command: nc -u 127.0.0.1 2013 and plaintext message: foo.garp.bung 42
--

local DataDaemon = require "L_DataDaemon"
local library    = require "L_DataLibrary"
local user       = require "L_DataUser"     -- user-defined processing
local json       = library.json()

local function method () error ("undeclared interface element", 2) end
local function interface (i) return setmetatable (i, {__newindex = method}) end


--local DataWatcher = interface {
  -- functions
  init              = method;       -- entry point
  -- info
  _AUTHOR           = "@akbooer";
  _COPYRIGHT        = "(c) 2013-2016 AKBooer";
  _DESCRIPTION      = "DataWatcher - Carbon relay daemon";
  _NAME             = "DataWatcher";
  VERSION           = "2016.04.09";
--}

local VERA      = DataDaemon.HOST            -- our own hostname
local conf_file = DataDaemon.config_path .. "DataWatcher.conf" 
local x_file    = DataDaemon.config_path .. "DataTranslation.conf"

local daemon                        -- our very own daemon
local relay                         -- our section of the carbon.conf file 
local config                        -- our own configuration parameters
local translate                     -- symbolic name to numeric lookup table

local CALL = "DataWatcherCallback"  -- callback name

local watched = {}                  -- set of watched variable tags 
local relayed = {}                  -- HTTP relayed variables
local UDP_relayed = {}              -- UDP relayed
local live_energy_usage = {}        -- latest energy info
local memory_stats = {}             -- system memory usage

local syslog                        -- syslog socket
local destinations                  -- destinations socket

local function series (dev, srv, var) return ("%03d.%s.%s"): format (dev or 0, srv or '?', var or '?') end
local function veraSeries (d,s,v) return table.concat ({VERA, series (d,s,v)}, '.') end

-- message in Whisper plaintext format: "path value timestamp"
local function plaintext (tag, value, time) return table.concat ({tag, value, time or os.time ()}, ' ') end

-- pass the outgoing message through any user-defined processing
-- possibly resulting in none, one, or more actual outgoing messages
local function relay_message (metric, value, time)
  for m,v,t in user.run (metric, value, time) do
    local message = plaintext (m, v, t)
    destinations: send (message)
  end
end

--------
--
-- LUUP utility functions for device variables 
--

-- Luup variable watch routines

-- System memory
local function getSystemFile (fname) 
  local line = ''
  local f = io.open (fname)  
  if f then line = f: read '*a' ; f: close() end
  return line
end

-- System memory
function _G.getSysinfo ()
  luup.call_delay ("getSysinfo", 120)    -- return two minutes later
	local info = {}
	local x = getSystemFile "/proc/meminfo"									-- memory use
	for a,b in x:gmatch '(%w+):%s+(%d+)' do	info[a] = b end
	if info.MemTotal and info.MemFree and info.Cached then
    info.MemUsed  = info.MemTotal - info.MemFree 
    info.MemAvail = info.Cached   + info.MemFree
    local time = os.time()
    local names = "MemAvail MemFree MemUsed"
    for name in names: gmatch "%w+" do
      memory_stats[name] = info[name]
      local tag = veraSeries (luup.device, "urn:system:serviceId:ProcMeminfo", name)
      relay_message (tag, info[name], time)
    end
  else
    memory_stats.NoMemInfo = "/proc/meminfo data not found"
  end
end

-- LiveEnergyUsage
-- luup.variable_set ("urn:micasaverde-com:serviceId:EnergyMetering1", "UserSuppliedWattage", Wattage, devNo)
function _G.DataEnergyWatcher ()
  luup.call_delay ("DataEnergyWatcher", 60)    -- return one minute later
  local time = os.time()
  local s, e = luup.inet.wget "http://127.0.0.1:3480/data_request?id=live_energy_usage"
  if s == 0 and e then
--    captures are: dev, name, room, cat, watts
    for dev, name, _, _, wattage in e: gmatch "(%d+)\t(.-)\t(.-)\t(%d+)\t(%d+)" do
      local devNo = tonumber (dev)
      local watts = tonumber (wattage)
      if devNo and watts then                     -- check that device currently exists
        local tag = veraSeries (devNo, "urn:micasaverde-com:serviceId:EnergyMetering1", "EnergyUsage")
        local kilowatthours = watts / 60000       -- convert Watt minutes to kWh
        relay_message (tag, kilowatthours, time)
        live_energy_usage[tag] = {name, watts}                         -- add energy usage to the log
      end
    end
  end
end

-- Watch callback
-- @params (lul_device, lul_service, lul_variable, lul_value_old, lul_value_new) 
_G[CALL] = function (lul_device, lul_service, lul_variable, _, lul_value_new) -- watch callback
  local tag = series (lul_device, lul_service, lul_variable)
  if watched[tag] then 
    watched[tag] = watched[tag] + 1   -- keep tally
    local whisperMessage = plaintext (tag, lul_value_new)
    syslog: send (whisperMessage)       -- SYSLOG receives the update without translation or user-defined processing
    local wildtag = tag: gsub ("^%d+", "*") 
    if translate[wildtag] then          -- do symbolic name translation for ALL devices (ie. "*", not "%d+")
      lul_value_new = translate[wildtag][lul_value_new] or 'unknown'
    end
    relay_message (veraSeries(lul_device, lul_service, lul_variable), lul_value_new)
  end
end

 -- save new configuration
local function save_conf_file ()
  local list = {}
  for w in pairs (watched) do list[#list+1] = w end
  table.sort (list)
  local f = io.open (conf_file, "w")
  if f then
    f: write (table.concat ({"[DataWatcher]", '#', '#'..os.date " configuration file auto update: %c", '#', ''}, '\n'))
    for _, x in ipairs (list) do f: write (table.concat {"watch=", x, '\n'}) end
    f: close()
  end
end

-- update watched variables with action "watch" or "nowatch"
local function updateWatch (action, info)      
  local seriesMatch1 = "^(%a+[%_%-])(%d+)%.(.*)"                      -- deal with optional prefix
  local seriesMatch2 = "^([%d%*]+)%.([^%.]+)%.([^%.]+)%.?(.*)"        -- allow wildcard devNo and variable suffix
  local changed = {}                                                  -- list of variables affected
--  captures are: prefix, serial, the_rest
  local _, _, b = info: match (seriesMatch1)
--  captures are: dev, srv, var, tail
  local dev, srv, var, _ = (b or info): match (seriesMatch2) 
  dev = tonumber(dev) or dev or '?'
  local deviceList = {[dev] = true}                                   -- single device number, or...
  if dev == "*" then deviceList = luup.devices end                    -- ...wildcard, all devices
  for dev in pairs (deviceList) do                                    -- loop through devices, applying action
    local value = luup.variable_get (srv, var, dev)
    if value then
      local tag = series(dev, srv, var)
      if action == "watch" and not watched[tag] then                  -- add to watched list
        luup.variable_watch(CALL, srv, var, dev)
        watched[tag] = 0
      elseif action == "nowatch" and watched[tag] then                -- remove from watched list
        watched[tag] = nil
      end
      changed[#changed+1] = action..'='..tag                          -- HTML response
    end
  end
  -- generate returned HTML for web request
  if #changed ~= 0 then
    table.sort (changed)
    return table.concat (changed,'\n')
  end
end

-- UDP relay: standard Carbon relay of message received on LINE_RECEIVER_PORT
local function UDPhandler (msg, ip) -- incoming Whisper plaintext message for relay to DESTINATIONS
  local path, value, timestamp = msg: match "(%S+)%s+(%S+)%s*(%S*)"
  if value then
    relay_message (path, value, timestamp)
    UDP_relayed[path] = (UDP_relayed[path] or 0) + 1   -- keep tally
  end
end

-- HTTP callback
local function HTTPhandler (action, info)        -- called with individual command line name/value pairs 
  local function noop (n,v) return (("Unknown request: %s=%s"): format (n,v or 'nil')) end
  local dispatch = {watch = updateWatch, nowatch = updateWatch}
  local html = (dispatch[action] or noop) (action, info) 
  save_conf_file ()
  return html
end

-- HTTP relay with AltUI Data Storage Provider functionality
function _G.HTTP_DataWatcherRelay (_,x) 
  local tag = x.target      -- supplied target name overrides device.serviceId.variable syntax
  if not tag then           -- ...we have to work out what it is
    local sysNo, devNo = (x.lul_device or ''): match "(%d+)%-(%d+)"
    if sysNo == "0" then    -- limit to local devices for the moment (because Vera id needs to be right)
      tag = veraSeries (tonumber(devNo), x.lul_service, x.lul_variable)
    end
  end
  relay_message (tag or '?', x.new, x.lastupdate)
  relayed[tag] = (relayed[tag] or 0) + 1   -- keep tally
  return "OK", "text/plain"
end

-- Initialisation

-- register DataYours as an AltUI Data Storage Provider
local function register_AltUI_Data_Storage_Provider ()
  local AltUI
  for devNo, d in pairs (luup.devices) do
    if d.device_type == "urn:schemas-upnp-org:device:altui:1" 
    and d.device_num_parent == 0 then   -- look for it on the LOCAL machine (might be bridged to another!)
      AltUI = devNo
      break
    end
  end
  
  if not AltUI then return end
  
  daemon.log ("registering with AltUI [" .. AltUI .. "] as Data Storage Provider")
  local ip = daemon.ip
  local newJsonParameters = {
    {
        default = "unknown",
        key = "target",
        label = "Metric Name",
        type = "text"
      },{
--        default = "http://"..ip..":3480/data_request?id=lr_render&target={0}&hideLegend=true&height=250&from=-y",
        default = "/data_request?id=lr_render&target={0}&hideLegend=true&height=250&from=-y",   -- 2016.02.26
        key = "graphicurl",
        label = "Graphic Url",
        type = "url"
      }
    }
  local arguments = {
    newName = "datayours",
    newUrl = "http://127.0.0.1:3480/data_request?id=lr_DataWatcherRelay&target={0}",
    newJsonParameters = json.encode (newJsonParameters),
  }

  luup.call_action ("urn:upnp-org:serviceId:altui1", "RegisterDataProvider", arguments, AltUI)
end

function init ()
  local _, wconfig = DataDaemon.read_conf_file (conf_file)  -- read our own configuration file (using index)
  local Name = next(wconfig)                -- pick up the first section (unspecified name)
  wconfig = wconfig[Name] or wconfig ["_anon_"] or {}   
  
  local _,xconfig  = DataDaemon.read_conf_file (x_file)     -- read symbolic translations file
  translate = xconfig
  for _, x in pairs (translate) do x.name = nil end                   -- expunge gratuitous name field
  
  daemon = DataDaemon.start {Name = "DataWatcher", HTTP_callback = HTTPhandler}  
  config = daemon.config
  config.DATAWATCHER = {
    VERSION = VERSION, 
    tally = {
      watched = watched, 
      HTTP_relayed = relayed,
      UDP_relayed = UDP_relayed,
    },
    translations = translate, 
    live_energy_usage = live_energy_usage,
    memory_stats = memory_stats,
  }
  
  relay = config["[relay]"] or {}
  syslog = daemon.open_for_syslog (relay.SYSLOG) 
  destinations = daemon.open_for_send (relay.DESTINATIONS)     -- UDP sender: uses [relay] DESTINATIONS
  
  daemon.log "starting variable watch..."
  local watchlist = wconfig.watch or {}
  if type (watchlist) == "string" then watchlist = {watchlist} end  -- convert string to list
  for _, var in ipairs (watchlist) do 
    updateWatch ("watch", var)                    -- start watching these variables
  end
  
  if relay.LIVE_ENERGY_USAGE == "1" then          -- start watching energy usage
    daemon.log "starting energy watch..."
    _G.DataEnergyWatcher() 
  end 
  
  if relay.MEMORY_STATS == "1" then               -- start watching memory stats
    daemon.log "starting memory watch..."
    _G.getSysinfo() 
  end 
  
  local UDP_port = tonumber (relay.LINE_RECEIVER_PORT) 
  if UDP_port then                                -- start listening for incoming UDP Whisper plaintext messages
    daemon.log ("starting UDP listener on port " .. UDP_port)
    daemon.open_for_listen (UDP_port, UDPhandler) 
  end
  
  luup.register_handler ("HTTP_DataWatcherRelay", "DataWatcherRelay")
  register_AltUI_Data_Storage_Provider ()         -- register with AltUI for data storage
end

----
