module ("L_DataGraph", package.seeall)

------------------------------------------------------------------------
--
-- DataGraph, lightweight Graphite Web-app look-alike
-- reading data from a Whisper database and rendering it in different ways
-- 
-- HTTP renderer is http://127.0.0.1:3480/data_request?id=lr_render&target=a.b.c&from=-2d
-- see https://graphite.readthedocs.org/en/latest/render_api.html
--  

local DataDaemon = require "L_DataDaemon"
local whisper    = require "L_DataWhisper"
local library    = require "L_DataLibrary"


local function method () error ("undeclared interface element", 2) end
local function interface (i) return setmetatable (i, {__newindex = method}) end


--local DataGraph = interface {
  -- functions
  init              = method;       -- entry point
  -- info
  _AUTHOR           = "@akbooer";
  _COPYRIGHT        = "(c) 2013-2016 AKBooer";
  _NAME             = "DataGraph";
  _VERSION          = "2016.01.04";
  _DESCRIPTION      = "DataGraph - Graphite Web-app look-alike";
--}


local daemon                    -- the daemon object with useful methods
local config                    -- all our configuration (and the .conf file info)
local graph                     -- our section of the carbon.conf file 
local ROOT                      -- for the whisper database

local plot  = {cpu = 0, number = 0}
local fetch = {cpu = 0, number = 0, points = 0}

local stats = { plot = plot, fetch = fetch}                -- interesting performance stats

local gviz    = library.gviz()

--
-- Date and Time formats
--

local function ISOdateTime (unixTime)       -- return ISO 8601 date/time: YYYY-MM-DDThh:mm:ss
  return os.date ("%Y-%m-%dT%H:%M:%S", unixTime)
end

local function UNIXdateTime (time)          -- return Unix time value for ISO date/time extended-format...   
--  if string.find (time, "^%d+$") then return tonumber (time) end
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

--TODO: Graphite format times

local function relativeTime  (time, now)     -- Graphite Render URL syntax, relative to current or given time
  local number, unit = time: match "^%-(%d*)(%w+)"
  if number == '' then number = 1 end
  local duration = {s = 1, min = 60, h = 3600, d = 86400, w = 86400 * 7, mon = 86400 * 30, y = 86400 * 365}
  if not (unit and duration[unit]) then return end      -- must start with "-" and have a unit specifier
  now = now or os.time()
  return now - number * duration[unit] * 0.998    -- so that a week-long archive (for example) fits into now - week 
end

--  utility functions 

-- iterator to expand a series list string "preamble{A,B,C}postamble{X,Y,Z}" 
-- into a list of individual series "preambleApostambleX", etc.
local function expansions (x)
  local function expand (x, z)
    z = z or ''
    local pre, braces, post = x: match "(.-)(%b{})(.*)"   
    if braces then
      for y in braces: gmatch "[^{},]+" do
        expand (post, table.concat {z, pre, y})
      end
    else
      coroutine.yield (z..x)  
    end
  end
  return coroutine.wrap (function () expand (x) end)
end

-------------
--
-- Data retrieval: from Whisper
--

-- return data for given series between t1 and t2
-- as well a whisper data object, return first non-nil data timestamp
local function get_whisper_data (series, t1, t2)
  local filename = table.concat {ROOT, series:gsub(':', '^'), ".wsp"}    -- change ":" to "^" and add extension 
  local cpu = daemon.cpu_clock ()
  local tv = whisper.fetch (filename, t1, t2)
  cpu = daemon.cpu_clock () - cpu
  if not tv then return "Series not found: " .. (series or '') end

  local n = tv.values.n or 0
  fetch.cpu    = fetch.cpu + (cpu - cpu % 0.001)
  fetch.number = fetch.number + 1
  fetch.points = fetch.points + n
  fetch.query  = ("Whisper query: CPU = %.3f mS for %d points"): format (cpu*1e3, n)
  daemon.log (fetch.query)

  return tv
end

-------------
--
-- plotting options - just a subset of the full Graphite Webapp set
-- see: http://graphite.readthedocs.org/en/latest/render_api.html
-- lineMode: slope [default], staircase, connected
--   slope     - line mode draws a line from each point to the next. Periods will Null values will not be drawn
--   staircase - draws a flat line for the duration of a time period and then a vertical line up or down to the next value
--   connected - Like a slope line, but values are always connected with a slope line, regardless of intervening Nulls
--
-- drawNullAs: (a small deviation from the Graphite Web App syntax)
--   null:    keep them null
--   zero:    make the zero
--   hold:    hold on to previous value
--
--  hideLegend: [false]
--   If set to true, the legend is not drawn. If set to false, the legend is drawn. 
--
-- areaMode: none, all, [not done: first, stacked]
-- 
-- vtitle: y-axis title
-- 
-- yMin/yMax: y-axis upper limit
-- 

-- return values for "mode" and "zero" plotting modes based on archive or input options
local function drawingModes (series, options)
  local mode, nulls
  local filename = table.concat {ROOT, series:gsub(':', '^'), ".wsp"}    -- change ":" to "^" and add extension 
  local action = {average = "connected", sum = "staircase", last = "staircase", max = "staircase", min = "staircase"}
  local method = {average = "null",      sum = "zero",      last = "hold",      max = "hold",      min = "hold"}
  -- try to determine the plotting mode from whisper file aggregation type
  local info = whisper.info (filename)              -- get the header data
  if info then 
    mode  = action[info.aggregationMethod] 
    nulls = method[info.aggregationMethod]
  end
  mode  = options.lineMode or mode                    -- command line options override defaults
  nulls = options.drawNullAs or nulls
  return mode, nulls
end

----
--
-- rendering function for SVG graphics
--

local function svgRender (options, t1, t2)
  -- note: this svg format is missing the embedded metadata object which Graphite includes
  local list = {}
  local mode, nulls, zero, hold, stair, slope, connect
  local data = gviz.DataTable ()
  data.addColumn('datetime', 'Time');
  local m, n = 0, 0
  local firstTimeAxis
  
  local aliases = {}
  if options.aliases then  -- strip out the individual alias names from the command line
    for alias in options.aliases: gmatch "[{,]([^{},]*)" do
      aliases[#aliases+1] = alias
    end
  end

  for series in expansions (options.target) do
    n = n + 1
    if n == 1 then     -- do first-time setup
      mode, nulls = drawingModes (series, options)
      stair   = (mode == "staircase")
      slope   = (mode == "slope")
      connect = (mode == "connected")
      hold    = (nulls == "hold")
      zero    = (nulls == "zero") and 0
      daemon.log (table.concat {"drawing mode: ", mode, ", draw nulls as: ", nulls})
    end
    list[n] = series
    data.addColumn('number', aliases[n] or series);
    local tv = get_whisper_data (series, t1, t2)  
    local timeAxis = table.concat (tv.times, ':')
    firstTimeAxis = firstTimeAxis or timeAxis
    if timeAxis ~= firstTimeAxis then return "incompatible time axes on multiple graphs" end
    m = tv.values.n

    local j = 0
    local previous
    for _, v,t in tv:ipairs() do
      j = j + 1
      if n == 1 then 
        if stair then data.addRow {t} end
        data.addRow {t} 
      end
      v = v or (hold and previous) or zero
      if stair then 
        data.setValue (j, n+1, previous); j = j + 1 
      end
      data.setValue (j, n+1, v) 
      previous = v 
    end
  end

  local cpu = daemon.cpu_clock ()
  local legend = "none"
  if not options.hideLegend and n < 6 then legend = 'bottom' end
  local title = options.title or options.target
  local opt = {title = title, height = options.height or 500, width = options.width, legend = legend, interpolateNulls = connect, backgroundColor = options.bgcolor}  

  local clip, vtitle
  if options.yMax or options.yMin then clip = {max = options.yMax, min = options.yMin} end
  if options.vtitle then vtitle = options.vtitle: gsub ('+',' ') end
  opt.vAxis = {title = vtitle, viewWindow = clip }
--  opt.crosshair = {trigger="selection", orientation = "vertical"}       -- or trigger = "focus"

  local chartType = "LineChart"
  if options.areaMode and (options.areaMode ~= "none") then chartType = "AreaChart" end
  local chart = gviz.Chart (chartType)
  local status = chart.draw (data, opt)
  cpu = daemon.cpu_clock () - cpu
  plot.cpu = plot.cpu + (cpu - cpu % 0.001)
  plot.number = plot.number + 1
  plot.render = ("render: CPU = %.3f mS for %dx%d=%d points"): format (cpu*1e3, n, m, n*m)
  daemon.log (plot.render)
  return status
end

----
--
-- rendering function for non-graphics formats
--

local function csvRender (p, t1, t2)
  -- this is the csv format that the Graphite Render URL API uses:
  --
  -- entries,2011-07-28 01:53:28,1.0
  -- ...
  -- entries,2011-07-28 01:53:30,3.0
  --
  local tv = get_whisper_data (p.target, t1, t2)  
  local data = {}
  for i, v,t in tv:ipairs() do
    data[i] = ("%s,%s,%s"): format (p.target, os.date("%Y-%m-%d %H:%M:%S",t), tostring(v) )
  end
  return table.concat (data, '\n')
end

local function jsonRender (p, t1, t2)
  -- this is the json format that the Graphite Render URL API uses
  --[{
  --  "target": "entries",
  --  "datapoints": [
  --    [1.0, 1311836008],
  --    ...
  --    [6.0, 1311836012]
  --  ]
  --}]
  local tv = get_whisper_data (p.target, t1, t2)  
  local data = {'[{', '  "target": "'..p.target..'"', ',  "datapoints": [' }
  local timeInfo, valueList = tv.times, tv.values
  local t, dt, n = timeInfo[1], timeInfo[3], (timeInfo[2]-timeInfo[1]) / timeInfo[3]
  local nocomma = {[n] = ''}
  for i = 1, n do
    data[#data+1] = table.concat {'  [', valueList[i] or 'null', ', ', t, ']', nocomma[i] or ','}
    t = t + dt
  end
  data[#data+1] = '  ]'
  data[#data+1] = '}]'
  return table.concat (data, '\n')
end

-- Render handler

local function renderHandler (p)    -- 'render' HTTP handler
  if not p then return '' end
  local function getTime (time)                        -- convert relative or ISO 8601 times as necessary
    if time then return relativeTime (time) or UNIXdateTime(time) end
  end

  local now = os.time()
  local t1 = getTime (p["from"])  or now - 24*60*60  -- default to 24 hours ago
  local t2 = getTime (p["until"]) or now
  
  if not p.target then return "No target specified" end
  
  local format = p.format or "svg"
  local reportStyle = {csv = csvRender, svg = svgRender, json = jsonRender}
  return (reportStyle[format] or svgRender) (p, t1, t2)
end

-- HTTP handler

function _G.HTTP_DataGraphRenderer (lul_request, ...)         -- renderer handler
  local ok, html = pcall (renderHandler, ...)  -- catch any errors which are reported in the html returned
  if not ok then daemon:error (html) end
  return html
end

-- Initialisation
function init ()
  daemon = DataDaemon.start {Name = "DataGraph"}
  config = daemon.config
  graph = config["[graph]"] or {}
     
  luup.register_handler ("HTTP_DataGraphRenderer", "render")             -- Graphite render handler
  luup.register_handler ("HTTP_DataGraphRenderer", "grafana/render")     -- for COMPATIBILITY with Grafana
    
  ROOT = graph.LOCAL_DATA_DIR     -- where to look for a database
  config.DATAGRAPH = {VERSION = _VERSION, whisper = ROOT, stats = stats}
end 

----
