module ("L_DataDash", package.seeall)

------------------------------------------------------------------------
--
-- DataDashboard - dashboard display of Whisper database
-- 
--  HTTP request to http://127.0.0.1:3480/data_request?id=lr_dashboard&page=home
--  ...starts the dashboard
-- 


local url        = require "socket.url"
local DataDaemon = require "L_DataDaemon"
local whisper    = require "L_DataWhisper"                      
local library    = require "L_DataLibrary"                       -- plotting & json

local function method () error ("undeclared interface element", 2) end
local function interface (i) return setmetatable (i, {__newindex = method}) end


--local DataDash = interface {
  -- functions
  init          = method;       -- entry point
  -- info
  _AUTHOR       = "@akbooer";
  _COPYRIGHT    = "(c) 2013-2016 AKBooer";
  _DESCRIPTION  = "Dashboard interface for DataYours and the Whisper database";
  _NAME         = "DataDash";
  _VERSION      = "2016.01.04";
--}


local gviz       = library.gviz()
local json       = library.json()

local PREFIX = "Vera-"    -- machine (and file) name prefix

local UI7 = luup.version_major == 7   -- true, if we're running UI7 

local daemon                    -- the daemon object with useful methods
local config                    -- all our configuration (and the .conf file info)
local dash                      -- dashboard section of the carbon.conf file for remote Vera IPs and Whisper location
local ROOT                      -- for the whisper database
local filePresent = {}          -- cache of existing file names

local conf_file   = DataDaemon.config_path .. "DataDash.conf"    -- out config file
local DataWatcher = "DataWatcher"                                -- to enable watch/nowatch for variables
local DG_render   = "render&bgcolor=F4F4F4"                      -- name of DataGraph renderer URL

local DataYours     -- device number

local DEBUG = { }   -- for diagnostics

local graphList = {}      -- read from / written to the conf_file

local icon_path                                                                       -- actual location
local default_icon_path = "/cmh/skins/default/icons/"                                 -- default UI5 location
if UI7 then default_icon_path = "/cmh/skins/default/img/devices/device_states/" end   -- default UI7 location

local default = {
  width = 950,                                              -- display size
  height = 600,
}

local context = {
  minColor = "LightSteelBlue",
  maxColor = "BurlyWood",
    
  graphIcons = {'Temperature', 'Humidity', 'Switch', 'Light', 'Energy', 'Security',  -- taken directly from dataMine
          'High setpoint', 'Low setpoint', 'Weather', 'Battery',
          'Time', 'System', 'Network', 'Plug', 'Remote Control',
          'Webcam', 'Fire', 'Computer', 'Counter', 'Curtains', 'Air'}, 
}

local VeraDevs   = {}                         -- table of Vera devices: VeraDevs[veraName][devNo] = {name = ..., category = ..., ...}

-- utility functions

-- Whisper Series object from series name string or list of elements
-- s = "vera.dev.srv.var" or 
-- s = {vera="vera-12345678", dev=nnn or "nnn", srv = "serviceId", var = "variable"}
local function series (s, s2)    
  s = s2 or s                                                               -- allow colon or dot notation
  local seriesMatch1 = "^(%a+[%_%-]%d+)%.(.*)"                              -- deal with optional prefix
  local seriesMatch2 = "^([%d%*]+)%.([^%.]+)%.([^%.]+)%.?(.*)"              -- allow wildcard devNo and variable suffix
  
  local function name ()
    local vera, devStr = '', s.dev
    if s.vera then  vera = s.vera .. '.' end
    if tonumber (devStr) then devStr = ("%03d"): format (devStr) end
    return ("%s%s.%s.%s"): format (vera, devStr, s.srv, s.var)
  end;
  
  if type (s) == "string" then
    local a,b = s
    s = {}
    s.vera, b = a: match (seriesMatch1)   
    s.dev, s.srv, s.var, s.tail = (b or a): match (seriesMatch2) 
  end
  
  s.name = name
  s.dev  = tonumber (s.dev) or s.dev                                        -- make it a number, if possible
  s.filename = function () return (name ()):gsub(":", "^") .. ".wsp" end    -- change ":" to "^" and add extension
  return setmetatable (s, {__tostring = name} )
end


local function directory (path)
  local dir ={}
  local f = io.popen ("ls "..path)
  if f then 
    for line in f:lines() do dir [#dir+1] = line end
    f: close ()
  end
  return dir
end

local function data_request (ip, request)
  -- Luup data_request using luup.inet.wget (was http.request)
  if ip == context.myIP then ip = "127.0.0.1" end   -- fix access problem, bypass ACL using local address
  request = table.concat {"http://", ip, request}  
  local  errmsg, info = luup.inet.wget (request)    
  if not info then daemon:error (errmsg) end 
  return info
end
  
local function friendlyName (ser)
  -- user-friendly translation of series name as "DeviceName - VariableName" (not unique, but not too bad)
  local s = series (ser) 
  local devName = ser
  if VeraDevs[s.vera] and VeraDevs[s.vera][s.dev] then devName = VeraDevs[s.vera][s.dev].name or s.dev end
  return table.concat {devName, ' - ', s.var}
end

local function get_vera_status (ip, request, plaintext)
  -- Luup request for status information, eg. sdata 
  local info = data_request (ip, ":3480/data_request?id=" .. request .. "&output_format=json")
  if info and not plaintext then info = json.decode (info) end
  return info
end
    
-- returns a directory list of the Whisper database   
-- metric = {vera, devNo, devStr, srv, var, series}   (devNo is a number, devStr is a string eg. "007")
local function get_whisper_metrics ()
  local metrics = {}
  local dir = directory (ROOT.."*.wsp")           -- look for whisper database files
--  DEBUG.DIRECTORY = {}            -- TODO: debug
  for _, file in ipairs (dir) do
    local d = file: gsub ("%^", ":")                   -- replace caret with colon
    local path = d: match "([^/]+).wsp$"               -- strip off directory path and extension
--    DEBUG.DIRECTORY[path] = true
    local s = series (path)
    if s.var then 
      local series = s:name ()
      local devStr = ("%03d"): format (s.dev)
      metrics[#metrics+1] = {vera = s.vera, devNo = tonumber(s.dev), devStr = devStr, srv = s.srv, var = s.var, series = series, seriesObject=s}
    end
  end
  return metrics
end

-- return full schema and aggregation information on Whisper metrics
-- adds {devName, xff, size, type, ret(entions), method, schema, Nsrv} to the basic metric info, (type is a shortened version of the serviceId)
local function get_augmented_whisper_metrics ()
  local metrics = get_whisper_metrics ()
  local Nsrv, services = 0, {}
  for _,m in ipairs (metrics) do
    local s = m.seriesObject
    local filename = ROOT .. s.filename()
    local info = whisper.info (filename)
    if info then
      local vDevs = VeraDevs[s.vera] or {}          -- may be missing if vera IP unknown
      if not services[s.srv] then                   -- find all the different services
        Nsrv = Nsrv + 1
        services[s.srv] = Nsrv
      end
      m.Nsrv    = services [s.srv]
      m.devName = (vDevs[s.dev] or {}).name or ("%03d"):format (s.dev)    -- use device number string as default
      m.xff     = ("%.1f"): format (info.xFilesFactor)
      m.type    = (s.srv: match ":(%a+)%d*$" or ''):gsub ("Sensor",'')      -- censor the "Sensor" word!        
--      m.type    = (s.srv: match ":(%a+)%d*$" or ''):gsub ("(%l)(%u)", "%1 %2")  -- put a space between camel-caps      
      m.ret     = tostring (info.retentions)
      m.method  = info.aggregationMethod
      m.schema  = ("%s [%s] %s"): format (m.ret, m.xff, m.method)
      local size = 0
      for _,a in ipairs (info.archives) do 
        size = size + a.size 
      end
      m.size = math.floor (size / 1024) 
    end
  end
  return metrics
end

local function showTooltip(code)      -- generic tooltip JavaScript
  local script = "showTooltip"        -- just return our own function name if no code given
  if code then  
    script = table.concat {"function ", script, [[(row, size, value) {
      var startDiv = '<div style="background:#fd9; padding:10px; font-family:Arial; font-size:10pt;" >'; 
      var endDiv   = '</div>';
      var _colour  = data.getValue(row, 3); 
      var _level   = data.getValue(row, 4);   // depth of treemap element 
      ]], table.concat(code), [[
      return startDiv + toolTip + endDiv;} 
     ]]}
  end
  return script
end
 
----
--
-- HTML utilities for server-side scripting
--

HTML = {

--local revisionDate = "2014.03.31"
--local version = revisionDate.."  @akbooer"

-- tostring() builds the HTML for nested element tables {type = {attr1 = "a1", attr2 = "a2", content1, content2, ...} }
tostring = function (element, buffer) 
  local list = buffer or {}
  local function p(y)
    for _, z in ipairs(y) do list[#list+1] = z end
  end
  local function html (element, level)
    if type (element) == "string" then p {element} return end
    local indent = '\n'..(' '):rep(level*2)
    local name, items = next (element)   
    p {indent, '<', name}
    if type(items) == "string" then items = {items} end
      for i,j in pairs (items) do
        if type(i) == "string" then   -- string items are element attributes
          p {' ', i, '="', j, '"'}
        end
      end
    p {'>'}
    if #items > 0 then                -- numeric items are (ordered) element contents
      for _,j in ipairs (items) do
        html (j, level+1)
      end
      p {indent,'</'.. name, '>'}     -- full terminator
    end
  end
  html (element, 0)
  local result = table.concat (list)
  return result
end;

page = function (parts)
  -- an HTML page with {head = ..., body = ...} parts
  return {html = {
      {head = parts.head or { {meta = {charset="utf-8"}} } }, 
      {body = parts.body or ''}
      } } 
end;

select = function (x)
  -- select {"name", opt1, opt2, ...}
  local html = {}
  function html:p(x) 
    if type (x) == "string" then x = {x} end
    for _, y in ipairs (x) do html[#html+1] = y end 
  end
  html:p {'<select'}
  for a,b in pairs (x) do 
    if type(a) ~= "number" then html:p {' ', a, '="', b, '"'} end
  end
  html:p '>'
  for _,y in ipairs (x) do html:p {'<option>', y, '</option>'} end
  html:p {'</select>', '\n'}
  return table.concat (html)
end;

input = function (type, name, value, title, extras)   
  -- generic input element (type, name, value, title, {other_attributes} )
  local z = {type=type, name=name, value=value, title=title}
  for i,j in pairs (extras or {}) do z[i] = j end
  return  {input = z}
end;

}

-- HTML utilities: handy HTML macros

local function div (x)      return {div = x}      end
local function fieldset (x) return {fieldset = x} end
local function input (x)    return {input = x}    end
local function form (x)     return {form = x}     end
local function p (x)        return {p = x}        end

-- input_type (name, value, extras)
local function hidden (...) return HTML.input ("hidden", ...) end
local function submit (...) return HTML.input ("submit", ...) end
local function radio  (...) return HTML.input ("radio",  ...) end
local function text   (...) return HTML.input ("text",   ...) end
local function label  (x,f) return {label = {["for"] = f, x}}   end

----
--
-- Metadata, build & maintain table of Vera devices: VeraDevs[veraName][devNo] = {name = ..., category = ..., ...}
--

local function refresh_vera_metadata ()
  -- recover metadata from the status .json of any known remote Veras
  -- VeraDevs[vera][dev#] = {devName, room, category}
  local function lookup (list)        -- lookup table x[id] = name
    local table = {}
    for _, x in ipairs (list or {}) do
      table[x.id or ''] = x.name or tostring (x.id)
    end
    return table
  end

  local knownVeras = {}                 -- list of known Vera IPs 
  for _, V in pairs (VeraDevs) do knownVeras[V.ip] = true end

  local listedVeras = {}                -- Veras from the conf file (if any)
  if dash and dash.VERAS then
    for ip in dash.VERAS: gmatch "%d+%.%d+%.%d+%.%d+" do listedVeras[ip] = true end
  end
  listedVeras[config.DAEMON.ip] = true   -- we do, at least, know our own address  
  
  for ip in pairs (listedVeras) do
    if not knownVeras[ip] then            -- we haven't got the configuration of this one yet
    daemon:log ("getting configuration of Vera at "..ip)
      local s = get_vera_status (ip, "sdata") or {}
      local e = get_vera_status (ip, "live_energy_usage", true) or ''   -- may be some hidden devices not in sdata
      if s.serial_number then                                           -- presume this is a genuine Vera response
        local vera = PREFIX .. s.serial_number: match "%d*"             -- there seems to be a trailing newline ?? so lose it.
        local rooms = lookup (s.rooms)
        local categories = lookup (s.categories)
        local V = {ip = ip}                           -- save the ip address for info
        for _, dev in ipairs (s.devices or {}) do
          local devNo = tonumber (dev.id)
          dev.name = dev.name or tostring (dev.id)
          dev.devNo = devNo
          dev.devStr = ("%03d"): format (devNo) -- three-digit string representation of devNo
          dev.vera = vera
          dev.room = tostring (rooms[dev.room or 0] or dev.room or "-no room-")
          dev.category = categories[dev.category or 0] or "-no category-" 
          V[devNo] = dev
        end
        for dev, name, room, cat, watts in e: gmatch "(%d+)\t(.-)\t(.-)\t(%d+)\t(%d+)" do   -- add any hidden devices reporting power
          local devNo = tonumber (dev)
          if not V[devNo] and watts then
            local dev = {}
            dev.name = '*' .. name                -- denote hidden with asterisk prefix 
            dev.devNo = devNo
            dev.devStr = ("%03d"): format (devNo) -- three-digit string representation of devNo
            dev.vera = vera
            dev.room = room
            dev.category = "-hidden-"   -- rather than the actual category, so it's obvious where this comes from 
            V[devNo] = dev
          end
        end
        VeraDevs[vera] = V
      end
    end
  end
end

local function get_vera_metadata ()
  -- get complete table of devices, and whether or not they are stored in the Whisper database
  -- {vera, dev, room, ip, name, category, stored}
  local function get_stored ()
    -- return lookup table of stored[vera][device] series in the Whisper database (ignoring serviceId and Variable)
    local stored = {}
    local metrics = get_whisper_metrics ()
    for _,x in pairs (metrics) do
      local s = x.seriesObject
      stored[s.vera] = stored[s.vera] or {}   -- create table if this is first reference
      stored[s.vera][s.dev] = true 
    end
    return stored
  end
  local data = {}
  local stored = get_stored ()
  for vera,devs in pairs (VeraDevs) do
    for i, d in pairs (devs) do
      if type (i) == "number" then
        local stored =  stored[vera] and stored[vera][i] 
        data[#data+1] = {vera = vera, room = d.room or '-no room-', ip = VeraDevs[vera].ip or '',
                        devName = d.name or d.devStr, category = d.category or '-no cat-', 
                        stored = stored, devStr = d.devStr, devNo = d.devNo}
      end
    end
  end
  return data
end
 
----
--  
-- Graphic dashboards (based on a treemap)
--

-- Generic TreeMap DataTable
-- TreeTable {data = data, root = "Vera", branches = {heirarchyStringList}, leaves = {otherStringList} }
-- expects {_label = x, _size = y, _colour (or _color) = z} in each element of data, although there are defaults

local function TreeTable (tree)
  local N = 0
  local t = gviz.DataTable ()
  local function newLeaf (parent, x, level)
    N = tostring(N + 1)
    local row = {{v = N, f = x._label or N}, parent._id, x._size or 1, x._colour or x._color or 0, level}
    for i, leaf in ipairs (tree.leaves or {}) do row[i+5] = x[leaf] end
    t.addRow (row)
    return {_id = N}
  end

  t.addColumn ("string", "_id")
  t.addColumn ("string", "_parent")
  t.addColumn ("number", "_size")
  t.addColumn ("number", "_colour")
  t.addColumn ("number", "_level")
  
  for _,x in ipairs (tree.leaves or {}) do 
    local y = (tree.data or {})[1] or {}    -- pull type from first element (if there)
    t.addColumn (type(y[x] or "string"), x) 
  end

  local root = newLeaf ({}, {_label = tree.root or '', _size = 0}, 0)          -- tree root

  for _,d in ipairs (tree.data or {}) do
    local branch = root
    for i,f in ipairs (tree.branches or {}) do
      local index = d[f]
      if index then 
        branch[index] = branch[index] or newLeaf (branch, {_label = index, _size = 0}, i)
        branch = branch [index]
      end
    end
    newLeaf (branch, d)
  end
  return t
end

local function databaseMap(options)
  -- whisper_metrics = {vera, devNo, devStr, srv, var, series}   (devNo is a number, devStr is a string eg. "007")
  -- augmented_whisper_metrics = above + {devName, xff, size, type, ret}
  local metrics = get_augmented_whisper_metrics ()
  for i, m in ipairs (metrics) do                   -- add TreeTable metadata
    m._label = m.devName
    m._colour = m.Nsrv
    m.type_var = table.concat {m.type, ' - ', m.var}
  end
  local tree = TreeTable {data = metrics, root = "Whisper Database", 
                            branches = {"vera", "type"}, leaves = {"type_var","schema", "devName","series", "devStr"} }
  local chart = gviz.TreeMap()
  local function render (t) 
    local from = {day = 'd', week = 'w', month = 'mon', year = 'y'}
    return table.concat { 
      [['<a target="Plot" href="/data_request?id=lr_]], DG_render, [[&from=-]], from[t], 
      [[&height=360&title=' + encodeURIComponent(data.getValue(row, 7) + ', ' + data.getValue(row, 5)) + ']],
      [[&target=' + data.getValue(row, 8) +'">]], t, [[</a>' +
      ]]} 
  end
  local function select (t) 
    return [['<a target="Plot" href="/data_request?id=lr_dashboard&page=graphs&select=' + data.getValue(row, 8) +'">select</a>' +]] 
  end
--  local more = [[
--        google.visualization.events.addListener(w, 'select', selectHandler);        
--        function selectHandler(e) {
--          alert('A table row was selected');
--        };
--  ]]
  local functions = showTooltip {[[
      var toolTip  = '#metrics: ' + size;  // for higher levels, it's just a metric count
      if (_level == null) { toolTip =  
         '<b>[' + data.getValue(row, 9) + '] ' + data.getValue(row, 7) + '</b>' + 
         '<br>' + data.getValue(row, 6) + 
         '<br>plot: ' + ]],
         render "day", "' / '+", render "week", "' / '+", render "month", "' / '+", select "select",  
         [['<br>' + data.getValue(row, 5) ;
       };
     ]]}
  local options = {
            height = options.height or default.height, 
            width = options.width, 
            maxDepth = 3, 
            generateTooltip = showTooltip, 
            minColorValue = 0, 
--            maxColorValue = Nsrv,
            minColor = "LightGreen", midColor = "LightCoral", maxColor = "CornflowerBlue",
--            minColor = '#8f8', midColor = '#f88', maxColor = '#88f',
          } 
  return chart.draw (tree, options, functions) 
end

local function colour_if_stored (stored)
  local colour = 0
  if stored then          -- stored
    colour = 2
  end
  return colour
end

local function devicesMap (options)
  local data = get_vera_metadata()
  for _,x in ipairs (data) do         -- add the TreeTable metadata
    x._label = x.devName
    x._colour = colour_if_stored (x.stored)
  end
  local treetable =  {data = data, root = "Devices", branches = {"vera", "category"}, leaves = {"devName", "devStr", "vera"} }
  local tree = TreeTable (treetable)
  local chart = gviz.TreeMap()

  local functions = showTooltip {[[
      var toolTip  = '#devices: ' + size;   // just a device count for higher levels
      if (_level == null) { toolTip =  
         '<b>[' + data.getValue(row, 6) + '] ' + data.getValue(row, 5) + '</b>' +
         '<br> <a href="/data_request?id=lr_dashboard&page=device&device=' + 
          data.getValue(row, 7) + '.' + data.getValue(row, 6) + '.*.*">]], 'view device variables', [[</a>' ;
       };
     ]]}
  local options = {
            height = options.height or default.height, 
            width = options.width, 
            maxDepth = 3, 
            generateTooltip = showTooltip, 
            minColorValue = 0, 
            maxColorValue = 2,
            minColor = context.minColor,
            maxColor = context.maxColor,
          } 
  return chart.draw (tree, options, functions)  
end

local function singleDeviceMap (options)
  -- get all the serviceIds and variables for this single device and show as tree
  local s = series (options.device) 
  if not s.var then return "invalid series syntax" end
  local devNo = tonumber (s.dev)
  local metrics = get_whisper_metrics ()
  local stored = {}
  for _, x in ipairs (metrics) do stored[x.seriesObject:name ()] = true end     -- build set of stored series
  local ip = VeraDevs[s.vera].ip or "127.0.0.1"

  local info = get_vera_status (ip, "status&DeviceNum="..devNo) or {}
  local devInfo = info["Device_Num_" .. devNo] or {}   -- data comes back in this weird table structure
  -- {id="1", service="urn:upnp-org:serviceId:TemperatureSensor1", variable="CurrentTemperature", value="13"}
  local devStates = devInfo.states or {}

  for i, x in ipairs (devStates) do   -- add TreeTable metadata
    s.srv = x.service
    s.var = x.variable
    local seriesName = s:name ()
    x.series = seriesName
    x.type   = ((x.service: match "serviceId:(%a+)%d*$") or x.service):gsub ("(%l)(%u)", "%1 %2")  -- put a space between camel-caps
    x._label = x.variable or x.id
    x._colour = colour_if_stored (stored[seriesName])
  end
  
  local d = VeraDevs[s.vera][devNo]
  local tree = TreeTable {data = devStates, root = (("[%s] %s"): format (d.devStr, d.name)) or options.device, 
                          branches = {"type"}, leaves = {"_label","service","value","series"} }
  local chart = gviz.TreeMap()
 
  local functions = showTooltip {[[
      var toolTip = '<b>' + data.getValue(row, 5) +'</b> <br>' + '#variables = ' + size; 
      var toolTip2 = ''; 
      if (_level == null) { 
        toolTip =  
        '<b>' + data.getValue(row, 5) + ' = ' + data.getValue(row, 7) + '</b>' +
        '<br>' + data.getValue(row, 6) + '<br>';
        if (data.getValue (row, 3) == 0) {
          toolTip2 = 
          '<a target="Plot" href="/data_request?id=lr_dashboard&page=archives&configure=' + data.getValue(row, 8) + '">watch</a>' ;
          }
        else {
          toolTip2 = 
          '<a href="http://]], ip, [[:3480/data_request?id=lr_]], DataWatcher, [[&watch=' + data.getValue(row, 8) + '">watch</a>' + ' / ' +
          '<a href="http://]], ip, [[:3480/data_request?id=lr_]], DataWatcher, [[&nowatch=' + data.getValue(row, 8) + '">nowatch</a>' ;
        };
        toolTip = toolTip + toolTip2;
       };
     ]]}
  local options = {
            height = options.height, 
            width = options.width, 
            maxDepth = 3, 
            generateTooltip = showTooltip, 
            minColorValue = 0, 
            maxColorValue = 2,
            minColor = context.minColor,
            maxColor = context.maxColor,
          } 
  return chart.draw (tree, options, functions) 
end

--------------
--
--

local function echo (o)
  local html = {}
  for i,j in pairs (o) do
    html[#html+1] = table.concat {i,'=',j}
  end
  return table.concat (html, ',')
end

-- get_rules (), returns results such as:
--  schema  = {name = "[default]",  retentions = "1h:7d"},                                    -- default to once an hour for a week
--  aggregation = {name = "[default]", xFilesFactor = 0.5, aggregationMethod = "average" },   -- these are the usual Whisper defaults anyway
local function get_rules (series)   
  local function match_rule (item, rules)
    -- return rule for which first rule.pattern matches item
    for i,rule in ipairs (rules) do
      if rule.pattern and item: match (rule.pattern) then
        return rule
      end
    end
  end
  local schemas     = DataDaemon.read_conf_file (ROOT .. "storage-schemas.conf")      -- re-read rule base every time in case it changed
  local aggregation = DataDaemon.read_conf_file (ROOT .. "storage-aggregation.conf")      
  -- apply the matching rules
  local schema = match_rule (series, schemas)  
  local aggr   = match_rule (series, aggregation) 
  return schema, aggr
end

-- dbcreate (options), create a Whisper database file
-- options = {
--      schema = "1m:1h,1h:90d",        -- typical schema definition, OR...
--      day = "5m", week = "1h", ...    -- separate sample rates with implied durations
--      series = "series name"
--      xff = 0.5, method = "average"   -- optional aggregation parameters
-- } 
local function dbcreate (o)
  local myIP = config.DAEMON.ip
  local css = [[
    a:hover         {background-color:Brown; }
    a               {width:200px; font-size:36pt; font-family:Arial; font-weight:bold; color:White; background-color:RosyBrown; text-align:center; 
                     border-radius:32px; padding:12px; margin:6px; float:left; text-decoration:none; text-valign:middle;}  
  ]]
  local schema = o.schema
  local function add (resolution, retention)
    if retention  then retention = ':' .. retention end 
    if resolution then schema[#schema+1] = resolution .. (retention or '') end
  end
  if not schema then    -- build it piece by piece
    schema = {}
    add (o.day, "1d")
    add (o.week, "7d")
    add (o.month, "30d")
    add (o.quarter, "90d")
    add (o.year, "1y")
    add (o.decade)        -- already has 5y or 10y suffix
    schema = table.concat (schema,',')
  end
  local s = series (o.series) 
  local filename = ROOT .. s:filename()  
  local ok,status = pcall (whisper.create, filename,schema,o.xff or 0,o.method)
  local header = whisper.info (filename)   -- get the header
  local html
  if ok and header then   
-- TODO: show archives and storage size
--    local ret = {}
--    for _, archive in ipairs (ret) do
--      ret = archive[2]
--    end
    local ip = VeraDevs[s.vera].ip
    local url = table.concat {"http://", ip, ":3480/data_request?id=lr_", DataWatcher, "&watch=", o.series}
    html = {
--             {img = {src = table.concat {"http://", myIP, "/cmh/skins/default/icons/DataWatcher.png"}}},
             {a = {href=url, "Click to WATCH this variable" }}}
  else
    daemon: error (status)
    html = table.concat {"Error creating: ", o.series, '<br>', "Status messsage = ", status}
  end
  return HTML.tostring (HTML.page {head = { {meta={charset="utf-8"}}, {style=css} }, body=html}, {"<!DOCTYPE html>"})
end

local function radiobar (group, buttons)
  local bar = {class = "radio-toolbar"}
  local function p (x) bar[#bar+1] = x end
  p {label = {{strong = {group}}, style="padding:4px 11px; width:72px; background:none"}}
  for i, name in ipairs (buttons) do
    if name == "blank" then
      p {label = {'&nbsp;', style="padding:4px 11px; width:48px; background:LightGray"}}
    else
      local lbl = name
      if name == '' then lbl = '-' end
      local id = "id" ..  (tostring({})):match "(%w+)$"         -- generate unique id
      p (radio (group, name, nil, {id = id, checked = ({"true"})[i]}))   -- first one is default
      p (label (lbl, id))
    end
  end
  return {div = {class="green", div (bar) }}
end

local function aggregationMenu (title)
  return div {class="blue", title,
        radiobar ("method", whisper.aggregationTypeToMethod),
--        radiobar ("xff", {"0.0", "0.2", "0.4", "0.6", "0.8", "1.0"}),
    }
end

local function archiveMenu (title)
  local x, b = '', "blank" 
  return div {class="blue", title,
        radiobar ("day", {x, "1s:1m,1m", "1m"}),
        radiobar ("week", {x,b, "5m", "10m"}),
        radiobar ("month", {x,b, "20m", "30m", "1h"}),
        radiobar ("quarter", {x,b,b,b, "1h"}),
        radiobar ("year",     {x,b,b,b, "3h", "6h"}),
        radiobar ("decade",    {x,b,b,b,b,b, "1d:5y", "1d:10y"}),
    }
end

local function archives (o)
  local css = [[
    body        {background:LightGray; padding:10px; font-family:Arial; font-size:10pt; }
    div         {vertical-align:middle; }
    img         {padding: 20px; }
    .radio-toolbar input[type="radio"] {display:none; }
    .radio-toolbar label:hover  {background-color:RosyBrown; }
    .radio-toolbar label { color: White; display:inline-block; background-color:LightGrey;
      padding:4px 11px; margin: 2px; width: 48px; text-align: center; }
    .radio-toolbar input[type="radio"]:checked + label {background-color:Brown;}  
    input.big:hover  {background-color:Brown; }
    input.big 
      {color: White; background: RosyBrown; padding:10px; margin:8px; font-weight:bold; width:72px; border:0px; border-radius:18px; }
    div.green {background:DarkSeaGreen; border-top-left-radius:16px; border-bottom-left-radius:16px; padding:1px 10px; margin:3px; }
    div.blue  {background:LightSteelBlue; border-top-left-radius:16px; padding:2px 10px 2px 30px; margin: 8px 10px 4px; }
    div.left = {float:left;}
    div.right = {float:right;}
  ]]
  local myIP = config.DAEMON.ip
  local series = o.configure
  local recommendation = ''
  local schema, aggr = get_rules (series)
  if schema then        -- go ahead, use the rules
    aggr = aggr or {}
    return dbcreate {series = series, schema = schema.retentions, method = aggr.aggregationMethod, xff = aggr.xFilesFactor}
  end
  -- otherwise build a complex HTML menu to specify Whisper parameters
  local html = {
    div { 
      form {action="data_request", method = "get", 
        hidden ("id", "lr_dashboard"),
        hidden ("page", "dbcreate"),
        hidden ("series", series), 
        div {style="float:left;",
--          {img = {src = table.concat {"http://", myIP, "/cmh/skins/default/icons/", "DataCache.png"}}},
          "<br>", submit ("create", "CREATE", "create whisper archives", {class = "big"}), 
          "<br>", input {type="Reset",  class="big"}, 
        },
        div {style = "float:left;",
          div {class = "left",
              aggregationMenu "Storage Aggregation",
            },
          "&nbsp; &nbsp; Whisper Database: ", series, 
          div { class = "left",
              archiveMenu " Storage Archives",
            }, 
          }
        }
      }, 
    div { style="clear:both; color:Brown", recommendation,
      {iframe = {name="createStatus", frameborder="0", width="800", height="35", " " }},
    },
  }
  return HTML.tostring (HTML.page {head = { {meta={charset="utf-8"}}, {style=css} }, body=html}, {"<!DOCTYPE html>"})
end

local function graphMap (options)
  -- treeMap of all stored Whisper graphs
  local seriesList, aliasList
  local iconLookup = {}
  for i,j in ipairs (context.graphIcons) do iconLookup[j] = i end
  local d = {}
  for _,graph in ipairs (graphList) do  
    local function plot (time)
      local from = {day = 1, week = 7, month = 30, quarter = 90, year = 365}
      return table.concat {'<a target="Plot" href="/data_request?id=lr_', DG_render, '&height=360&target=', 
                                seriesList, "&title=", url.escape(graph.name or ''), "&aliases=", (aliasList or ''), 
                                "&lineMode=", graph.lineMode or '', "&areaMode=", graph.areaMode or '', "&drawNullAs=", graph.drawNullAs or '',
                                "&vtitle=", graph.vtitle or '', "&yMin=", graph.yMin or '', "&yMax=", graph.yMax or '',
                                "&from=-", from[time], 'd">', time, '</a>'}
    end
    local chan, alias, list = {}, {}, {}
    for k,series in ipairs (graph) do
      chan[k]  = series
      list[k]  = friendlyName (series) 
      alias[k] = url.escape (friendlyName (series) )
    end
    seriesList = '{' .. table.concat (chan,  ',') .. '}'
    local list = table.concat (list, '<br>')
    aliasList =  '{' .. table.concat (alias, ',') .. '}' 
    local plots = table.concat {"plot: ", plot "day", ' / ', plot "week", ' / ', plot "month", ' / ', plot "quarter", ' / ', plot "year" }
    d[#d+1] = {_label=graph.name, _color= iconLookup[graph.icon] or 0, icon=graph.icon, plot=plots, list=list}
    -- Channels
  end
  local tree = TreeTable {data = d, root = "Whisper Graphs", 
                            branches = {"icon"}, leaves = {"_label","plot","list"} }
  local functions = showTooltip {[[
      var toolTip  = '#graphs: ' + size;  // for higher levels, it's just a metric count
      if (_level == null) { toolTip =  
         '<b>' + data.getValue(row, 5) + '</b><br>' + 
          data.getValue(row, 6) + '<br>' +
          data.getValue(row, 7) ;
       };
     ]]}
  local chart = gviz.TreeMap()
  local opt = {width = options.width, height = options.height or 300,
            allowHtml = true,
            maxDepth = 3, 
            generateTooltip = showTooltip, 
            minColorValue = 0, 
            maxColorValue = #context.graphIcons,
            minColor = "LightGreen", midColor = "LightCoral", maxColor = "CornflowerBlue",
  }
  return chart.draw (tree, opt, functions)
end


local selectionList = {}

local function selectionTable (o)
  -- show graphing selection list
  local data = {'<div style="padding-left:10px; padding-right:10px;"><b>Selection List:</b><br>'}
  for _,x in ipairs (selectionList) do
    data[#data+1] = friendlyName (x)
  end
  data[#data+1] = '</div>'
  return table.concat (data, '<br>')  
end

----------
--
-- Graphing HTML page 
--

local function write_conf_file (path, data)
  -- write a Graphite conf file given data structure from reading one
  local fh = io.open (path, 'w')
    if fh then
      fh: write (table.concat ({'#', '#'..os.date " configuration file auto update: %c", '#', ''}, '\n'))
      for _, item in ipairs (data) do
        if item.name then
          fh: write(table.concat {'[', item.name or '', ']\n'})
          for parameter, value in pairs (item) do
            if parameter ~= "name" then
              fh: write(table.concat {parameter, '=', tostring(value), '\n'})
            end
          end
        end
      end
      fh: close ()
    end
end

-- managed saved plots and dispatch special plots
local function graphingPage (o)

  local unpack = unpack or table.unpack     -- Lua v5.1 / v5.2 difference

  local lines = fieldset {{legend = "Line Options", width="200px;"},
    "<p>Line Mode:&nbsp;",
    HTML.select {name="lineMode", title="line plotting mode", style="float:right;", value='', '', "slope", "staircase", "connected"},    
    "<p>Area Mode:&nbsp;",      -- could be "none", "first", "all", "stacked", but not yet all implemented
    HTML.select {name="areaMode", title="area plotting mode", style="float:right;", value='', '', "none", "all"},    
    "<p>Draw Null As:&nbsp;",
    HTML.select {name="drawNullAs", title="how to handle null values", style="float:right;", value='', '', "null", "zero","hold"},    
  }
  
  local vaxis = fieldset {{legend = "Vertical Axis"},
    "<p>Y label:&nbsp;",
    text ("vtitle", '', "vertical axis label", {style="float:right;"} ),    
    "<p>Y min:&nbsp;",
    text ("yMin", '', nil, {style="float:right;"} ),    
    "<p>Y max:&nbsp;",
    text ("yMax", '', nil, {style="float:right;"} ),    
   }

  local graphOpts = fieldset {{legend = "Graph Options"},
    "Icon Group:",
    HTML.select {name="icon", title="graph icon group", style="float:right;", value=selectionList.icon or '', unpack (context.graphIcons)},
    "<p>Name:",
    text ("graphname", "", nil, {style="float:right;"}), 
    "<p>&nbsp;",
    submit ("action", "Save Whisper Graph",   "save selection list as graph"),
    submit ("action", "Clear Selection List", "clear all selection list items"),
  }
  
  local graphOptionsForm = {
    action="data_request", method = "get",
    hidden ("id",   "lr_dashboard"),
    hidden ("page", "graphs"), "<br>",
    div {lines}, 
    div {vaxis}, 
    graphOpts
  }

  local graphIndex = {}     -- create index of graph names
  for i, g in ipairs (graphList) do
    if g.icon and g.name  
      then graphIndex[g.name] = i end
  end
--  DEBUG.graphList  = graphList  -- TODO: debug
    
  -- add / clear item on graph selection list and present graphing panel
  if o.select   then selectionList[#selectionList+1] = o.select end  
  if o.clear    then selectionList = {} end
--  if o.load     then selectionList = graphList[graphIndex[o.load] or ''] or {} end  
  
  local graphName = (o.graphname or ''): gsub ('+', ' ')          -- text input gives '+' for ' '
  selectionList.name = graphName or selectionList.name or ''
  for name,value in pairs (o) do
    if not ("action/graphname/page"):match (name) then               -- configure plot with menu options
      selectionList[name] = o[name] or selectionList[name]
    end
  end
  
  local a = o.action
  if a then
    if a: match "^Clear" then selectionList = {} end 
    if a: match "^Save" and o.graphname then
      if #selectionList == 0 then
        table.remove (graphList, graphIndex[graphName])
      elseif not graphIndex[graphName] then
        table.insert (graphList, selectionList)
        selectionList = {}
      end
      write_conf_file (conf_file, graphList)
    end
  end
  
  -- display form
  
  local css = [[
    body        {background:LightGray; padding:10px; font-family:Arial; font-size:10pt; }
    div         {vertical-align:middle; float: left; margin-right=8px;}
    div.icon    {width=70; }
    img         {padding: 20px; }
  ]]

  local head = { {meta = {charset="utf-8"}}, {style = css} }
  local body = {
    div { 
--      fieldset {
--        {legend = "Graphs"},
--        div {class = "icon", {img = {alt="DataGraph", src="http://127.0.0.1:80/cmh/skins/default/icons/DataGraph.png"}} }, --TODO: fix icon
        div { {form = graphOptionsForm} },
        div {selectionTable () },
--      }
    }
  } 

  return HTML.tostring (HTML.page {head = head, body = body}, {"<!DOCTYPE html>"})
end


local function homePage (o)
 
  local css = [[ 
    body            {font-family:Arial;  font-size:10pt; background:LightGray; } 
    div             {vertical-align:middle; clear:both; }
    span.blank      {width:60px; float:left; }
    a:hover         {background-color:Brown; }
    a               {width:100px; font-weight:bold; color:White; background-color:RosyBrown; text-align:center; 
                     padding:2px; margin:6px; margin-left:0; float:left; text-decoration:none;}
    a.left          {border-top-left-radius:20px; border-bottom-left-radius:20px; }
    a.right         {border-top-right-radius:20px; border-bottom-right-radius:20px; }
    a.rhs           {float:right;}
    iframe          {border:none; width:100%;}
    iframe.top      {border-top-left-radius:24px; }
    iframe.bottom   {border-bottom-left-radius:24px;}
    #Menu           {margin-left:auto; margin-right:auto; width:80%; }
    #Maps           {background:LightSteelBlue; padding:8px; padding-top:0; border-bottom-left-radius:30px;  width:100%;}
    #Plots          {background:Tan; padding:8px; padding-bottom:0; border-top-left-radius:30px; width:100%;}
  ]]


  local menu = {id = "Menu",
          {a = {target="Maps", href="/data_request?id=lr_dashboard&height=300&page=whisper", title="tree map of Whisper data series", class="left", "Whisper" }},
          {a = {target="Maps", href="/data_request?id=lr_dashboard&height=300&page=devices", title="tree map of all devices", "Devices" }},
          {a = {target="Maps", href = "/data_request?id=lr_dashboard&page=graphmap", title = "stored Whisper graphs", class="right", "Graphs"}}, 
        }
  if context.datamine and context.datamine ~= '' then                   -- add some more buttons for dataMine
    menu[#menu+1] = {span = {class="blank", "&nbsp;"}}
    menu[#menu+1] = {a = {target="Maps", href="/data_request?id=lr_dmDB&height=300&report=dataMine", title="tree map of dataMine data channels", 
                  class="left", "dataMine" }}
    menu[#menu+1] = {a = {target="Maps", href = "/data_request?id=lr_dmDB&report=graphmap", title = "stored dataMine graphs", class="right", "dM Graphs"}}
  end
  
  menu[#menu+1] = 
    {a = {target = "_blank", href = "/data_request?id=lr_dashboard&page=configure", title="DataYours configuration", class="left right rhs", "Configuration"}}
  
  local home = {
    head = { {meta = {charset="utf-8"}}, {title = "DataYours"}, {style = css} },
    body= {
      div {
        div {id = "Maps", {iframe = {name="Maps", height="320px", " "}} },
        div (menu),
        div {id="Plots", {iframe = {class="top", name="Plot", height="400px", " "}} }
      }
    }
  } 
  return HTML.tostring (HTML.page (home), {"<!DOCTYPE html>"})
end


local function schemaTable (options) 
local file = ROOT.."storage-schemas.conf"
  local data = gviz.DataTable ()
  local conf = DataDaemon.read_conf_file (file) 
  data.addColumn ("number", "Priority")    
  data.addColumn ("string", "Name")    
  data.addColumn ("string", "Pattern <regex>")    
  data.addColumn ("string", "Storage Schema")    
  for i,x in ipairs (conf) do
    data.addRow {i, x.name, (x.pattern or ''): gsub ("%%","\\\\"), (x.retentions or ''): gsub (",",", ")}
  end
  local chart = gviz.Table()
  return chart.draw (data, {width = options.width or 650, height = options.height})   
end

local function methodsTable (options) 
  local file = ROOT.."storage-aggregation.conf" 
  local data = gviz.DataTable ()
  local conf = DataDaemon.read_conf_file (file) 
  data.addColumn ("number", "Priority")    
  data.addColumn ("string", "Name")    
  data.addColumn ("string", "Pattern <regex>")    
  data.addColumn ("string", "xff")    
  data.addColumn ("string", "Aggregation Method")    
  for i,x in ipairs (conf) do
    data.addRow {i, x.name, (x.pattern or ''): gsub ("%%","\\\\"), x.xFilesFactor, x.aggregationMethod}
  end
  local chart = gviz.Table()
  return chart.draw (data, {width = options.width or 650, height = options.height})   
end

local function metricsTable (options)
  local data = gviz.DataTable ()
  data.addColumn ("string", "Vera")  
  data.addColumn ("string", "Device No.")  
  data.addColumn ("string", "Device Name")  
  data.addColumn ("string", "Type")  
  data.addColumn ("string", "Variable")  
  data.addColumn ("string", "Storage Schema")
  data.addColumn ("number", "xff")    
  data.addColumn ("string", "Aggregation Method")    
  data.addColumn ("number", "Size (kB)")  
  -- whisper_metrics = {vera, devNo, devStr, srv, var, series}   (devNo is a number, devStr is a string eg. "007")
  -- augmented with {devName, xff, size, type, ret(entions), method, schema, Nsrv} 
  local metrics = get_augmented_whisper_metrics ()
  for _, m in ipairs (metrics) do
      data.addRow {m.vera, m.devStr, m.devName, m.type, m.var, tostring(m.ret), m.xff, m.method, m.size}
  end
  local chart = gviz.Table()
  return chart.draw (data, {width = options.width or default.width, height = options.height})   
end

local function configuration (options)
  -- configuration panel for all system daemon parameters (local and remote)
  
  local myIP = config.DAEMON.ip
  local css = [[ 
    body          {font-family:Arial; font-size:10pt; background:LightGray;} 
    iframe        {border:none; background:LightGray; }
    iframe.top    {border-top-left-radius:16px;}
    iframe.bottom {border-bottom-left-radius:16px;}
    p.vera        {float:left; text-align:center; }
    #Buttons      {margin-left:auto; margin-right:auto; width:80%; }
    #Local        {background:LightSteelBlue; float:left; padding:8px; padding-left:36px; padding-top:0; border-bottom-left-radius:30px; width:100%; border-top:0;}
    #Remote       {background:Tan; float:left; padding:8px; padding-left:36px; border-top-left-radius:30px; width:100%;}
    div.inner {float:left; background:DarkSeaGreen; padding:4px; padding-left:16px; text-align:center; vertical-align:middle; margin-left:16px; margin-bottom:8px;}
    div.local       {border-bottom-left-radius:18px; padding-top:0;}
    div.remote      {border-top-left-radius:18px; }
    img:hover       {background-color:White; }
    img             {margin:4px; padding:2px;}
    a.button:hover  {background-color:Brown; }
    a.icon          {text-decoration:none;}
    a.button        {width:100px; font-weight:bold; color:White; background-color:RosyBrown; text-align:center; vertical-align:middle; 
                       padding:2px; margin:6px; margin-left:0; float:left; text-decoration:none;}
    a.left          {border-top-left-radius:20px; border-bottom-left-radius:20px; }
    a.right         {border-top-right-radius:20px; border-bottom-right-radius:20px; }
    a.rhs           {float:right;}
  ]]


  local function panel(daemon, ip)
    local name = daemon: match "%a+" 
    local url = table.concat {"http://", ip, ":3480/data_request?id=lr_", daemon, "&page=config"}
    local content = {img = {src = table.concat {"http://", myIP, icon_path, name, ".png"}, alt = name}}
    return {a = {target="config", class="icon", href= url, title= name.." configuration page", content }}
    end

  local function vera (class, veraName, ip, N) 
    local daemon = {"DataWatcher", "DataCache", "DataGraph", "DataDash", "DataMineServer"}
    label = label or {} 
    veraName = veraName or ''
    local veraDiv = {class = "local inner", table.concat {class, "&nbsp;", veraName, "&nbsp;(", ip, ")<br>"}}
    for i = 1, N or #daemon do
      if (i ~= #daemon) or (context.datamine ~= '') then
        veraDiv[#veraDiv+1] = panel(daemon[i], ip)
      end
    end
    return div (veraDiv)
  end
  
   -- configuration start
  
  local remoteVeras, veraNames = {}, {} 
  for veraName, info in pairs (VeraDevs) do 
    veraNames[info.ip] = veraName
    if info.ip ~= myIP then remoteVeras [#remoteVeras+1] = info.ip end
  end
  table.sort (remoteVeras)

  local localVera= vera("local: ", veraNames[myIP], myIP)  
  local remoteDiv = {id = "Local", localVera}
  for _,ip in ipairs (remoteVeras) do remoteDiv[#remoteDiv+1] = vera("remote: ", veraNames[ip] or "unknown", ip, 3) end

  local menu = {id = "Buttons", 
          {a = {target="config", href="/data_request?id=lr_dashboard&page=schemas", title="storage schema rules", class="button left", "Schemas" }},
          {a = {target="config", href="/data_request?id=lr_dashboard&page=metrics", title="storage metrics directory", class="button", "Metrics" }},
          {a = {target="config", href="/data_request?id=lr_dashboard&page=methods", title="storage aggregation rules", class="button right", "Aggregation"}}, 
          div { style = "float:right; vertical-align:middle; ", "icons by ", {a = {href = "http://icons8.com/license/", "icons8"}}} , 
        }

  local config2 = {
    head = { {meta = {charset="utf-8"}}, {title = "DataYours Config"}, {style = css} },
    body= {
      div {
        div (remoteDiv), div (menu),
        div {id = "Remote", " " },
        {iframe = {name="config", src="/data_request?id=lr_DataDash&amp;page=config", width="1000px", height = "600px", " "}} 
        }
      }
    }

  return HTML.tostring (HTML.page (config2), {"<!DOCTYPE html>"})
end

local dispatch = {schemas = schemaTable, methods = methodsTable, metrics = metricsTable,
                  whisper = databaseMap, devices = devicesMap, 
                  dbcreate = dbcreate, frame = homePage, home=homePage,  
                  graphs = graphingPage, graphmap = graphMap, configure=configuration, 
                  archives = archives, device = singleDeviceMap, echo = echo}

local function dashboard (options)
  local Nopt = 0
  for _ in pairs (options) do Nopt = Nopt + 1 end
  if Nopt == 0 then options = {page = "home"} end     -- go to home page if command is simply "dashboard" with no parameters
  refresh_vera_metadata ()            -- refresh our system knowledge

  local action = dispatch[((options.page or ''):match "%w*") or '']  -- allows page.subpage syntax
  if action then return action (options) end
  local request = {"DataDash: unknown request: "}
  for i,j in pairs(options) do request[#request+1] = table.concat {'&', i, "=", j} end
  local html = table.concat (request)
  return html
end

-- HTTP handler

function _G.HTTP_DataDashRenderer (lul_request, lul_parameters)         -- dashboard renderer handler
  local ok, html = pcall (dashboard, lul_parameters)    -- catch any errors which are reported in the html returned
  if not ok then daemon:error (html) end
  return html
end


-- Initialisation
function init ()  
  -- Device parameter signifies that we're a child device in DataYours and will get our own device configuration
  daemon = DataDaemon.start {Name = _NAME}
  config = daemon.config

  -- DataDash is a Dashboard interface for DataYours, 
  -- providing views of Vera(s) Devices, Variables, Graphs, etc...
  
  config["[dash]"] = config["[dash]"] or {}           -- might be missing if no config file was present
  dash = config["[dash]"]

  ROOT = dash.LOCAL_DATA_DIR                          -- where to look for a database
  context.datamine = dash.DATAMINE_DIR                -- whether to show dataMine buttons
  icon_path = dash.ICON_PATH: match "%S+" or default_icon_path      -- where to look for icons
  
  graphList = DataDaemon.read_conf_file (conf_file)   -- read saved graphs
  get_whisper_metrics()
  
  config.DATADASH = {VERSION = _VERSION, whisper = ROOT}
  config.DEBUG = DEBUG 
  
  local url_fmt = '<a href="http://%s:3480/data_request?id=lr_dashboard" %s>dashboard</a>'
  local launch_URL = url_fmt: format (config.DAEMON.ip, 'target="_blank"')
 
  luup.register_handler ("HTTP_DataDashRenderer", "dashboard")       -- dashboard render handler

  daemon:log "DataDash7 daemon started" 
end

----

