module ("L_DataMineServer", package.seeall)

local LICENSE       = [[
  Copyright 2016 AK Booer

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

------------------------------------------------------------------------
--
-- dmDBserver, modified to run as a DataYours daemon.
--
--dmDB_handler() responds to requests like:
-- [your_Vera_url]:3480/data_request?id=lr_getSearchKeyRange
--&format=csv
--&DeviceNum=7
--&serviceId=urn:upnp-org:serviceId:Dimming1
--&Variable=Test
--&From=42    -- or ISO-8601 extended date/time format YYYY-MM-DDTHH:MM:SS or ordinal date YYYY-DDD (1-366)
--&To=88
-- see: http://wiki.micasaverde.com/index.php/Luup_Lua_extensions#function:_register_handler
-- and: http://forum.micasaverde.com/index.php/topic,17499.msg136838.html#msg136838


local DataDaemon = require "L_DataDaemon"
local dmDB       = require "L_DataBaseDM"
local library    = require "L_DataLibrary"


local function method () error ("undeclared interface element", 2) end
local function interface (i) return setmetatable (i, {__newindex = method}) end


--local DataMineServer = interface {
  -- constants
  colours = {
     minColor = "LightSkyBlue", 
     midColor = "Khaki", 
     maxColor = "LightCoral",
   };
  -- functions
  init              = method;       -- entry point
  -- info
  _AUTHOR           = "@akbooer";
  _COPYRIGHT        = "(c) 2013-2015 AKBooer";
  _NAME             = "DataMineServer";
  _VERSION          = "2016.02.08";
  _DESCRIPTION      = "DataMineServer - dmDBserver, modified to run as a DataYours daemon";
--}


local cli  = library.cli()
local gviz = library.gviz()
local json = library.json() 

local daemon                    -- the daemon object with useful methods
local config                    -- all configuration (and the .conf file info)
local datamineserver            -- our configuration only
local mine                      -- our section of carbon.conf

local dm, dmDBstatus
local Variables, Graphs

local plot  = {cpu = 0, number = 0}
local fetch = {cpu = 0, number = 0, points = 0}
local stats = { plot = plot, fetch = fetch}                -- interesting performance stats

-- dmDBserver, modified to run as a DataYours daemon.

----
--dmDB_handler() responds to requests like:
-- [your_Vera_url]:3480/data_request?id=lr_getSearchKeyRange
--&format=csv
--&DeviceNum=7
--&serviceId=urn:upnp-org:serviceId:Dimming1
--&Variable=Test
--&From=42    -- or ISO-8601 extended date/time format YYYY-MM-DDTHH:MM:SS or ordinal date YYYY-DDD (1-366)
--&To=88
-- see: http://wiki.micasaverde.com/index.php/Luup_Lua_extensions#function:_register_handler
-- and: http://forum.micasaverde.com/index.php/topic,17499.msg136838.html#msg136838


----
--
-- Utilities
-- 

local function log (text)
  daemon:log (text) 
end

local function ISOdateTime (unixTime)       -- return ISO 8601 date/time: YYYY-MM-DDThh:mm:ss
  return os.date ("%Y-%m-%dT%H:%M:%S", unixTime)
end

local function UNIXdateTime (time)          -- return Unix time value for number string or ISO date/time extended-format...   
  if string.find (time, "^%d+$") then return tonumber (time) end
  local field   = {string.match (time, "^(%d%d%d%d)-?(%d?%d?)(-?)(%d?%d?)T?(%d?%d?):?(%d?%d?):?(%d?%d?)") }
  if #field == 0 then return end
  local name    = {"year", "month", "MDsep", "day", "hour", "min", "sec"}
  local default = {0, 1, '-', 1, 12, 0, 0}
  if #field[2] == 2 and field[3] == '' and #field[4] == 1 then  -- an ORDINAL date: year-daynumber
    local base   = os.time {year = 2000, month = 1, day = 1}
    local offset = ((field[2]..field[4]) -1) * 24 * 60 * 60
    local fixed  = os.date ("*t", base + offset)
    field[2] = fixed.month
    field[4] = fixed.day
  end
  local datetime = {}
  for i,j in ipairs (name) do
    if not field[i] or field[i] == ''
      then datetime[j] = default[i]
      else datetime[j] = field[i]
    end
  end
  return os.time (datetime)
end

local function relativeTime  (time, now)     -- Graphite Render URL syntax, relative to current or given time
  local number, unit = time: match "^%-(%d*)(%w+)"
  if number == '' then number = 1 end
  local duration = {s = 1, min = 60, h = 3600, d = 86400, w = 86400 * 7, mon = 86400 * 30, y = 86400 * 365}
  if not (unit and duration[unit]) then return end      -- must start with "-" and have a unit specifier
  now = now or os.time()
  return now - number * duration[unit] * 0.998    -- so that a week-long archive (for example) fits into now - week 
end

local function range (t1,t2, n)
  local times = {}
  local dt = (t2 - t1) / (n - 1)
  local t = t1
  for i = 1,n do
    times[i] = math.floor (t+0.5); t = t + dt
  end
  return times
end

local function resample (cursor, times)
  local f  = {}
  local t1, v
  local prev = cursor:getSearchKeyRange {t = times[1]}
  if not prev then return f end
  for i,target in ipairs (times) do
    v, t1 = cursor:getSearchKeyRange {t = target}
    if not v then 
      v = prev
    elseif t1 ~= target then
      v = cursor.getPrev ()
    end
    f[i] = v
    prev = v
  end
  return f
end

local function filter (key, V)  -- apply dataMine filters and offset to plot variables
  local match = dmDB.search (Variables, key)
  if #match == 1 then
    local C = match[1]
    local offset = C.DataOffset or 0
    local limit = C.FilterEnable and C.FilterEnable == 1
    local lower, upper = C.Filterminimum or -math.huge, C.FilterMaximum or math.huge
    for i,v in ipairs(V) do
      local v2 = v + offset
      if limit then
        if v2 > upper then v2 = upper
        elseif lower and v2 < lower then v2 = lower 
        end
      end   
      V[i] = v2 
    end
  end
end

local function channelName (key)  -- return name given key = {Device = c.Device, Service = c.Service, Variable = c.Variable}
  local name
  local channelInfo = dmDB.search (Variables, key)
  if #channelInfo == 1 
    then name = channelInfo[1].Name 
    else name = ("dev = %03d, srv = %s, var = %s"): format (key.Device, key.Service:match "%w*$", key.Variable)
  end
  return name
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

---------------
--
-- Actions
--

local function timeline (options)
  local d = gviz.DataTable ()
  d.addColumn ('string', 'Channel Name')
  d.addColumn ('date', 'Start')
  d.addColumn ('date', 'End')
  for _,v in pairs (Variables) do
    local key = {Id = v.Id}
    local dmc, status = dm:openCursor {key = key}
    if dmc then
      local name = ("%s : %d.%s.%s"): format (v.Name or '?', v.Device or 0, v.Service or '?', v.Variable or '?')
      local _, first = dmc:getFirst ()
      local _, last  = dmc:getLast ()
      if first and last then
        if last <= first then last = first + 1 end
        d.addRow {name, first, last}
      end
      dmc: close ()
    else
      daemon:error (status)
    end
  end  
  local t = gviz.Chart "Timeline"
  local opt = {width = options.width or 800, height = options.height or 700}
  return t.draw (d, opt)
end

local function directory (options)
  local d = gviz.DataTable ()
  d.addColumn ('number', 'Id')
  d.addColumn ('string', 'Channel Name')
  d.addColumn ('string', 'Service')
  d.addColumn ('string', 'Variable')
  d.addColumn ('number', 'Device No.')
  for _,j in pairs (Variables) do
    d.addRow {j.Id, j.Name, j.Service: match "([^:]*)$", j.Variable, j.Device}
  end
  local t = gviz.Table ()
  local opt = {width = options.width or 750, height = options.height or 700}
  return t.draw (d, opt)
end


local function graphMap (options)
  local icon = {'Temperature', 'Humidity', 'Switch', 'Light', 'Energy', 'Security',
          'High setpoint', 'Low setpoint', 'Weather', 'Battery',
          'Time', 'System', 'Network', 'Plug', 'Remote Control',
          'Webcam', 'Fire', 'Computer', 'Counter', 'Curtains', 'Gas'}
  local function plot (i, time)
    local from = {day = 1, week = 7, month = 30, quarter = 90}
    return table.concat {'<a target="Plot" href="/data_request?id=lr_dmDB&height=300&graph=', i, "&from=-", from[time], 'd">', time, '</a>'}
  end
  local d = {}
  for i,j in pairs (Graphs) do  
    local chan = {}
    for k,c in ipairs (j.Channels) do
      local key = {Device = c.Device, Service = c.Service, Variable = c.Variable}
      chan[k] = channelName (key)
    end
    chan = table.concat (chan, '<br>')
    local plots = table.concat {"plot: ", plot (i,"day"), ' / ', plot (i,"week"), ' / ', plot (i,"month"), ' / ', plot (i,"quarter")}
    d[#d+1] = {_label=j.Name, _color=j.Icon, icon=icon[j.Icon] or tostring(j.Icon), plot=plots, chan=chan}
    -- Channels
  end
  local tree = TreeTable {data = d, root = "dataMine Graphs", 
                            branches = {"icon"}, leaves = {"_label","chan","plot"} }
  local functions = showTooltip {[[
      var toolTip  = '#graphs: ' + size;  // for higher levels, it's just a metric count
      if (_level == null) { toolTip =  
         '<b>' + data.getValue(row, 5) + '</b><br>' + 
          data.getValue(row, 7) + '<br>' +
          data.getValue(row, 6) ;
       };
     ]]}
  local chart = gviz.TreeMap()
  local cols = colours
  local opt = {width = options.width, height = options.height or 300,
            allowHtml = true,
            maxDepth = 3, 
            generateTooltip = showTooltip, 
            minColorValue = 0, 
            maxColorValue = #icon,
            minColor = cols.minColor, midColor = cols.midColor, maxColor = cols.maxColor,  
  }
  return chart.draw (tree, opt, functions)
end


local function graphs (options)
  local icon = {'Temperature', 'Humidity', 'Switch', 'Light', 'Energy', 'Security',
          'High setpoint', 'Low setpoint', 'Weather', 'Battery',
          'Time', 'System', 'Network', 'Plug', 'Remote Control',
          'Webcam', 'Fire', 'Computer', 'Counter', 'Curtains', 'Gas'}
  local d = gviz.DataTable ()
  d.addColumn ('number', 'Id')
  d.addColumn ('string', 'Icon')
  d.addColumn ('string', 'Graph')
  d.addColumn ('string', 'Duration (Days)')
  d.addColumn ('string', 'Channels')
  for i,j in pairs (Graphs) do  
    local chan = {}
    for k,c in ipairs (j.Channels) do
      local key = {Device = c.Device, Service = c.Service, Variable = c.Variable}
      chan[k] = channelName (key)
    end
    chan = table.concat (chan, '<br>')
    local plot = table.concat {'<a href="/data_request?id=lr_dmDB&height=300&graph=', i, '">', j.Name, '</a>'}
    d.addRow {i, icon[j.Icon] or j.Icon, plot, j.Period / 86400, chan}
    -- Channels
  end
  local more 
--  more = [[
--        google.visualization.events.addListener(w, 'select', selectHandler);        
--        function selectHandler(e) {
--          alert('A table row was selected');
--        };
--  ]]
  local t = gviz.Table ()
  local opt = {width = options.width or 800, height = options.height, allowHtml = true,}
  return t.draw (d, opt, more)
end


local function graph (p, times, options)
  local g = Graphs[p]     -- p guaranteed to be numeric, thanks to cli.parser
--  local y = dmDB.search (Graphs, {Name = "Vera Mem"})
  if not g then return "no such graph" end
  local now = os.time()
  local ago = now - g.Period
  local rng
  if times.t1  then     -- override default graph timescale with specified interval
    rng = range (times.t1, now, 24*30+1)
  else
    rng = range (ago, now, 24*30+1)
  end
  local d = gviz.DataTable ()
  d.addColumn ('datetime', 'Time')
  local chan = g.Channels
  local firstTime = true
  for k,c in ipairs (chan) do
    local key = {Device = c.Device, Service = c.Service, Variable = c.Variable}
      local dmc, status = dm:openCursor {key = key}
      log ('dmDB: Cursor status = '..status)
    d.addColumn ('number', channelName (key)) 
      if dmc then 
        local v = resample (dmc, rng)
        filter (key, v)
        if firstTime then 
          firstTime = false
          for j,t in ipairs (rng) do d.addRow {t, v[j]} end
        else
          local col = k+1
          for j in ipairs (rng) do d.setValue (j, col, v[j]) end
        end
      dmc:close ()
      end
  end
  local t = gviz.LineChart ()
  local opt = {title = g.Name, height = options.height or 600, legend = "bottom"}
  return t.draw (d, opt)
end

local function chart  (dmc, times, chartType, options) 
  local N = 0
  if not dmc then return 'error opening database cursor' end
  local data = gviz.DataTable ()
  data.addColumn('datetime', 'Time');
  data.addColumn('number', 'Value');
  for n, v,t in dmc:searchKeyRange (times) do N=n; data.addRow {t,v} end          
  local chart = gviz.Chart (chartType)
  local title = {}
  local info = dmc:get() or {}
  for i,j in pairs (info) do title[#title+1] = table.concat {i,'=',tostring(j)} end
  title = table.concat (title, ', ')
--  title = ("Id = %d, Name = %s"): format (info.Id or 0, info.Name or '')
  local opt = {title = title, height = options.height or 500, width = options.width, legend = 'none'}         
  return chart.draw (data, opt), N
end

-------------
--
-- TreeMap
--


-- dataMineMap() treemap of dataMine channels
local function dataMineMap(options)
  local metrics = {}
  local types, Nsrv = {}, 0
  -- cf. whisper_metrics = {vera, devNo, devStr, srv, var, series}   (devNo is a number, devStr is a string eg. "007")
  -- and augmented_whisper_metrics = above + {devName, xff, size, type, ret}
  for _,j in pairs (Variables) do
    local devNo = j.Device
    local m = {devNo = devNo, devStr = tostring (devNo), schema = "dataMine #"..j.Id, series=j.Id}
    m.devName = tostring (j.Id)
    m.type = (j.Service: match "(%a*)%d*$"): gsub ("Sensor",'')
    if not types[m.type] then
      Nsrv = Nsrv + 1
      types[m.type] = Nsrv
    end
    m.id = j.Id
    m.var = j.Variable
    m._label = j.Name
    m._colour = types[m.type]
    m.type_var = table.concat {m.type, ' - ', m.var}
    metrics[#metrics+1] = m
  end
  local tree = TreeTable {data = metrics, root = "dataMine Database", 
                            branches = {"type"}, leaves = {"type_var","id", "_label","schema", "devStr"} }
  local chart = gviz.TreeMap()
  local function render (t) 
    local from = {day = 'd', week = 'w', month = 'mon', year = 'y'}
    return table.concat { 
      [['<a target="Plot" href="/data_request?id=lr_dmDB&from=-]], from[t], 
      [[&height=360&title=' + encodeURIComponent(data.getValue(row, 7) + ', ' + data.getValue(row, 5)) + ']],
      [[&plot=' + data.getValue(row, 6) +'">]], t, [[</a>' +
      ]]} 
  end
  local functions = showTooltip {[[
      var toolTip  = '#metrics: ' + size;  // for higher levels, it's just a metric count
      if (_level == null) { toolTip =  
         '<b>[' + data.getValue(row, 9) + '] ' + data.getValue(row, 7) + '</b>' + 
         '<br>dataMine channel #' + data.getValue(row, 6) +
         '<br>plot: ' + ]],
         render "day", "' / '+", render "week", "' / '+", render "month", 
         [['<br>' + data.getValue(row, 5) ;
       };
     ]]}
  local options = {
            height = options.height or 500, 
            width = options.width, 
            maxDepth = 3, 
            generateTooltip = showTooltip, 
            minColorValue = 0, 
            maxColorValue = Nsrv,
            minColor = colours.minColor, midColor = colours.midColor, maxColor = colours.maxColor,  
          } 
  return chart.draw (tree, options, functions) 
end

-------------
--
-- Handler to dispatch actions
-- @params: (lul_request, lul_parameters, lul_outputformat)
--
function _G.HTTP_dmDBhandler (_, lul_parameters)
  local days = 24 * 60 * 60
  local seconds = {day = days, week = 7*days, month=31*days, year = 365*days}
  local function getTime (time)                        -- convert relative or ISO 8601 times as necessary
    if time then return relativeTime (time) or UNIXdateTime(time) end
  end

  local function query ()
      local p, status = cli.parse (lul_parameters)
      if not p then return status end

      local t = p.times
      local now = os.time()
      local t1,t2
      if t.t1 then t1 = getTime (t.t1) or now - 24*60*60 end -- default to 24 hours ago
      if t.t2 then t2 = getTime (t.t2) or now end
      if t.dt then                  -- adjust t1,t2 accordingly
        local dt = seconds[t.dt]
        if         t1 and not t2 then t2 = t1 + dt 
        elseif     t2 and not t1 then t1 = t2 - dt
        elseif not t1 and not t2 then 
          t2 = now
          t1 = t2 - dt
        end                     -- ignore 'dt' if 't1' and 't2' both specified
      end
      local times = {t1 = t1, t2 = t2}

      p.other = p.options
      if p.actions.report == "channels" then return directory (p.other) end
      if p.actions.report == "timeline" then return timeline (p.other) end
      if p.actions.report == "graphs"   then return graphs (p.other) end
      if p.actions.report == "graphmap" then return graphMap (p.other) end
      if p.actions.report == "dataMine" then return dataMineMap (p.options) end

--      cpu = os.clock () - cpu
--      plot.cpu = plot.cpu + (cpu - cpu % 0.001)
--      plot.number = plot.number + 1
--      plot.render = ("render: CPU = %.3f mS for %dx%d=%d points"): format (cpu*1e3, n, m, n*m)
--      syslog:send (plot.render)

      if p.actions.plot  then return (chart  ((dm:openCursor {key = {Id = p.actions.plot}}), times, "LineChart",  p.other)) end
      if p.actions.graph then return graph (p.actions.graph, times, p.other) end

      local dmc, status = dm:openCursor {key = p.searchKeys}
      if dmc then
        local N = 0
        local cpu = os.clock()
        status = {}
      if not p.options.format or p.options.format == "csv" then   -- default
        for n, v,t in dmc:searchKeyRange (times) do N = n; status[n] = ("%d,%g"): format (t,v) end 
        elseif p.options.format == "iso" then
        for n, v,t in dmc:searchKeyRange (times) do N=n; status[n] = ("%s,%g"): format (ISOdateTime(t),v) end 
      elseif type (gviz) == 'table' then
        status[1], N = chart (dmc, times, p.options.format, p.other)
      end 
      status [N+1] = '\n'   -- make sure it's non-empty 
      status = table.concat (status, '\n')
      daemon:log (("query: CPU = %.3f mS for %d points"): format ((os.clock()-cpu)*1e3, N))
      dmc:close ()
      end    
      return status
  end    
  local ok, result = pcall (query)    -- don't want any error raised during handler code! 
  if not ok then daemon:error (result) end
  return result
end

-- CLI_init, defines the HTTP command line interface syntax for the server
local function CLI_init ()
  cli = cli.parser "&start=2013-12-01&device=123&report=log&format=Table"
  
  cli.parameter ("times", "dt", {"dt","interval"}, {"day","week","month","year"}, "duration")
  cli.parameter ("times", "t1", {"t1", "start", "from"}, "string", "start times in unix or ISO format")
  cli.parameter ("times", "t2", {"t2", "stop",  "to", "until"},   "string", "stop times in unix or ISO format")
  
  cli.parameter ("searchKeys", "Device",    "devicenum", "number", "Luup device number")
  cli.parameter ("searchKeys", "Service",   "serviceid", "number", "Luup serviceId")
  cli.parameter ("searchKeys", "Variable",  "variable",  "string", "Luup variable name")
  cli.parameter ("searchKeys", "Id",        "channel",   "number", "dataMine channel id")
  cli.parameter ("searchKeys", "Name",      "name",      "string", "dataMine channel name")
  
  cli.parameter ("options",    "format",    "format",  {"csv","iso","Table","LineChart","AreaChart"}, "report format")
  cli.parameter ("options",    "width",     "width",   "number",    "HTML output width")
  cli.parameter ("options",    "height",    "height",  "number",    "HTML output height")
  
  cli.parameter ("actions",    "report",    "report",    {"channels","graphs", "timeline", "dataMine", "graphmap"},  "various report types")
  cli.parameter ("actions",    "plot",      "plot",      "number",  "plot specified dataMine channel")
  cli.parameter ("actions",    "graph",     "graph",     "number",  "plot specified dataMine graph")
  cli.parameter ("actions",    "target",    "target",    "^(%w+).(%w+).([%w%_]+)$",  "Graphite/Whisper format plot target: device.service.variable")
end

------------
--
-- DataYours calls init() if this daemon required
--

function init ()
  daemon = DataDaemon.start {Name = _NAME}
  config = daemon.config
  
  config.DATAMINESERVER = {VERSION = _VERSION, stats = stats}
  datamineserver = config.DATAMINESERVER
  mine = config["[mine]"] or {}
  
  CLI_init ()
  dm, dmDBstatus = dmDB.open {json = json, database = mine.DATAMINE_DIR, 
                                  maxpoints = tonumber (mine.MAXPOINTS) or 2000}
  datamineserver.database = {version = dmDB._VERSION, status = dmDBstatus}
  if dm then 
    log 'getting database configuration...'
    local info = dm:get()
    Variables = info.Variables
    Graphs = info.Graphs
  else
    daemon:error (dmDBstatus)
  end
  luup.register_handler ("HTTP_dmDBhandler", "dmDB")
  
  local n,m = 0, 0
  for _ in pairs (Variables or {}) do n = n + 1 end
  for _ in pairs (Graphs or {}) do m = m + 1 end
--  daemon.display (n.. " channels", m.. " graphs")
  
  daemon:log "DataMineServer daemon started" 

end 

----
