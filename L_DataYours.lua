ABOUT = {
  NAME            = "DataYours";
  VERSION         = "2017.09.18";
  DESCRIPTION     = "DataYours - parent device for Carbon daemons";
  AUTHOR          = "@akbooer";
  COPYRIGHT       = "(c) 2013-2017 AKBooer";
  DOCUMENTATION   = "",
  LICENSE         = [[
  Copyright 2013-2017 AK Booer

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
]]
}
------------------------------------------------------------------------
--
-- DataYours - a simple device front-end for the DataDaemons 
--

-- 2015-12-15   new version - no child devices
-- 2016-04-11   add LINE_RECEIVER_PORT to relay parameters
-- 2016-04-14   add DATAMINE_DIR to graph parameters
-- 2016.07.02   add new WebAPI module reference (to replace DataGraph in future)

local DataDaemon = require "L_DataDaemon"
local lfs        = require "lfs"

-- thanks to @amg0 for ALTUI update
local ALTUI = {   -- generic display variables 
  srv = "urn:upnp-org:serviceId:altui1",
  var1 = "DisplayLine1",
  var2 = "DisplayLine2",
}

local DataYoursSID = "urn:akbooer-com:serviceId:DataYours1"

-- device variable names for user-defined parameters
local conf = "CONFIG_DIR"             -- path to carbon configuration files
local dm   = "DATAMINE_DIR"           -- location of DataMine directory
local dest = "DESTINATIONS"           -- Watcher sends to these locations
local dir  = "LOCAL_DATA_DIR"         -- location of the Whisper database
local erg  = "LIVE_ENERGY_USAGE"      -- relay energy usage if "1"
local icon = "ICON_PATH"              -- URL path to icons (ie. without /www/ root)
local mem  = "MEMORY_STATS"           -- relay system memory info
local sys  = "SYSLOG"                 -- relay data to syslog
local udp  = "UDP_RECEIVER_PORT"      -- Cache listens on this port (for UDP)
local line = "LINE_RECEIVER_PORT"     -- relay listens on this port (for UDP)
local vera = "VERAS"                  -- list of remote Veras

local function log (message)
  luup.log ('DataYours: '.. (message or '???') )
end


-- LUUP utility functions 

-- UI7 return status : {0 = OK, 1 = Device config error, 2 = Authorization error}
local function set_failure (status)
  if (luup.version_major < 7) then status = status ~= 0 end        -- fix UI5 status type
  luup.set_failure(status)
end

local function getVar (name, service, device) 
  service = service or DataYoursSID
  device = device or luup.device
  local x = luup.variable_get (service, name, device)
  return x
end

local function setVar (name, value, service, device)
  service = service or DataYoursSID
  device = device or luup.device
  local old = getVar (name, service, device)
  if tostring(value) ~= old then 
   luup.variable_set (service, name, value, device)
  end
end

-- get and check UI variables
local function uiVar (name, default, lower, upper)
  local value = getVar (name) 
  local oldvalue = value
  if value and (value ~= "") then           -- bounds check if required
    if lower and (tonumber (value) < lower) then value = lower end
    if upper and (tonumber (value) > upper) then value = upper end
  else
    value = default
  end
  value = tostring (value)
  if value ~= oldvalue then setVar (name, value) end   -- default or limits may have modified value
  return value
end

-----------
--
-- heartbeat monitor for memory usage (and other things)
--TODO: add database healthcheck and metric count


-- check the database access and count the metrics
local function check_whisper_db (path)
  local n = 0
  for file in lfs.dir (path) do 
    if file:match "%.wsp$" then n = n + 1 end  -- only count .wsp archives
  end
  return n
end

function DataYoursPulse (path)
  luup.call_delay ("DataYoursPulse", 5*60, path)        -- periodic pulse (5 minutes)
  
  if path: match "%S+" then 
    local ok, metrics = pcall (check_whisper_db, path)
    if ok then metrics = metrics .. " metrics" end
    setVar (ALTUI.var2, metrics, ALTUI.srv)  
  end

  local AppMemoryUsed =  math.floor(collectgarbage "count")         -- app's own memory usage in kB
  setVar ("AppMemoryUsed", AppMemoryUsed) 
  collectgarbage()                                                  -- tidy up a bit
end

-----------
--
-- set up daemons 
-- 

local daemonInfo = -- module filename for each daemon
  {
    Watch = "L_DataWatcher",
    Cache = "L_DataCache",
    Graph = "L_DataGraph",
    Dash  = "L_DataDash",
    Mine  = "L_DataMineServer",
  }

local function create_daemons (daemons)
  local dlist = {}
  for daemon, filename in pairs (daemonInfo) do  
    if daemons:match (daemon) then
      log (table.concat {" service: ", daemon, " - ", filename})
      local d = require (filename)
      local ok, err = pcall (d.init)          -- launch the daemon
      if ok 
        then dlist[#dlist+1] = daemon
        else log (err) end
    end
  end
  return table.concat (dlist, ' ')
end
 
----
--
-- DataYours
--

function Startup ()
  log "starting..."
  
  -- get configuration parameters from device variables  
  
  do -- version number
    local y,m,d = ABOUT.VERSION:match "(%d+)%D+(%d+)%D+(%d+)"
    local version = ("%d.%d.%d"): format (y%2000,m,d)
    setVar ('Version', version)
  end
  
  setVar ('StartTime', os.date())
  setVar (ALTUI.var1, "no services", ALTUI.srv)
  setVar (ALTUI.var2, "no database", ALTUI.srv)

  local daemons = uiVar ("DAEMONS",   '')           -- eg. "Watch, Cache, Graph, Dash, Mine"
  
  local parameters = {
    [conf] = uiVar (conf, "/www/"),
    [dm]   = uiVar (dm,   ''),                      -- location of DataMine directory
    [dest] = uiVar (dest, "127.0.0.1:2003"),        -- Watcher sends to these locations
    [dir]  = uiVar (dir,  ""),                      -- location of the Whisper database (eg. /nas/whisper/)
    [erg]  = uiVar (erg,  ''),                      -- relay energy usage if "1"
    [icon] = uiVar (icon, "/cmh/skins/default/img/devices/device_states/"),
    [line] = uiVar (line, ''),                      -- relay UDP listener port
    [mem]  = uiVar (mem,  ''),                      -- relay system memory usage
    [sys]  = uiVar (sys,  ''),                      -- relay data to syslog, eg. "172.16.42.112:514"
    [udp]  = uiVar (udp,  "2003"),                  -- Cache listens on this port (for UDP)
    [vera] = uiVar (vera, ''),                      -- list of remote Veras
  }

  -- build selection of parameters values for each section of the configuration 'file'
  local function select (params)
    local s = {}
    for _, x in pairs (params) do s[x] = parameters[x] end
    return s
  end
  
  DataDaemon.set_config {                 -- override the carbon config with these sections
    relay = select {sys, dest, erg, mem, line},
    cache = select {udp, dir},
    graph = select {dir, dm},
    dash  = select {vera, dir, dm, icon},
    mine  = select {dm},
  }
  local conf_path = parameters[conf]: match "%S+"
  if conf_path then
    DataDaemon.config_path = conf_path    -- set path for the (other) config files
  end
  
  log "starting heartbeat"
  DataYoursPulse (parameters[dir])         -- start the heartbeat (monitors memory and DB usage)
  log "starting services"
  local dlist = create_daemons (daemons)
  
  log "...startup complete"
  if dlist: match "%S+" then setVar (ALTUI.var1, dlist, ALTUI.srv) end
  set_failure (0)                             -- 0 = OK, 1 = authorization fail, 2 = fatal error
  return true, "OK", ABOUT.NAME
end


-----
