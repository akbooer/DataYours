module ("L_DataGraph", package.seeall)

local ABOUT = {
  NAME            = "DataGraph";
  VERSION         = "2016.10.04";
  DESCRIPTION     = "DataGraph - Graphite Web-app look-alike";
  AUTHOR          = "@akbooer";
  COPYRIGHT       = "(c) 2013-2016 AKBooer";
  DOCUMENTATION   = "",
  LICENSE       = [[
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
}

------------------------------------------------------------------------
--
-- DataGraph, lightweight Graphite Web-app look-alike
-- reading data from a Whisper database and rendering it in different ways
-- 
-- HTTP renderer is http://127.0.0.1:3480/data_request?id=lr_render&target=a.b.c&from=-2d
-- see https://graphite.readthedocs.org/en/latest/render_api.html
--  

-- 2016.04.11   refactoring of svg rendering in advance of Storage Finders
--              add mixed timebase support: no more "incompatible time axes on multiple graphs"
-- 2016.04.12   remove legend limit of 5 (thanks @ d55m14, although it will still limit to 10.)
--              add graphType parameter (also thanks @ d55m14)

local DataDaemon = require "L_DataDaemon"
local whisper    = require "L_DataWhisper"
local library    = require "L_DataLibrary"


local daemon                    -- the daemon object with useful methods
local config                    -- all our configuration (and the .conf file info)
local graph                     -- our section of the carbon.conf file 
local ROOT                      -- for the whisper database

local plot  = {cpu = 0, total = 0}
local fetch = {cpu = 0, total = 0, points = 0}

local stats = {plot = plot, fetch = fetch}                -- interesting performance stats

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
  local message  = ("Whisper query: CPU = %.3f mS for %d points"): format (cpu*1e3, n)
  fetch.cpu    = fetch.cpu + (cpu - cpu % 0.001)
  fetch.total = fetch.total + 1
  fetch.points = fetch.points + n
  daemon.log (message)

  return tv, message
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
-- graphType: line is default, but otherwise specify any Chart type: BarChart, ColumnChart, ... (not PieChart)

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
  -- note: this svg format does not include Graphite's embedded metadata object
  local list = {}
  local mode, nulls, zero, hold, stair, slope, connect
  local data = gviz.DataTable ()
  data.addColumn('datetime', 'Time');
  local m, n = 0, 0
  
  local aliases = {}
  if options.aliases then  -- strip out the individual alias names from the command line
    for alias in options.aliases: gmatch "[{,]([^{},]*)" do
      aliases[#aliases+1] = alias
    end
  end

  -- fetch the data
  
  local row = {}   -- rows indexed by time
  local whisper_stats = {}
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
    local tv, stats = get_whisper_data (series, t1, t2)  
    whisper_stats['Q'..n] = stats      -- string() so that the pretty-printed log looks good
    
    local current, previous
    for _, v,t in tv:ipairs() do
      row[t] = row[t] or {t}                        -- create the row if it doesn't exist
      current = v or (hold and previous) or zero    -- special treatment for nil
      row[t][n+1] = current                         -- fill in the column
      previous = current
    end
  end
  fetch.query = whisper_stats
  
  -- sort the time axes
  
  local index = {}
  for t in pairs(row) do index[#index+1] = t end    -- list all the time values
  table.sort(index)                                 -- sort them
  m = #index
  
  -- construct the data rows for plotting
  
  local previous
  for _,t in ipairs(index) do
    if stair and previous then
      local extra = {}
      for a,b in pairs (previous) do extra[a] = b end   -- duplicate previous
      extra[1] = t                                      -- change the time
      data.addRow (extra)
    end
    data.addRow (row[t])
    previous = row[t]
  end
  
  -- add the options
  
  local legend = "none"
  if not options.hideLegend then legend = 'bottom' end
  local title = options.title or options.target
  local opt = {
    title = title, 
    height = options.height or 500, 
    width = options.width, 
    legend = legend, 
    interpolateNulls = connect, 
    backgroundColor = options.bgcolor
  }  

  local clip, vtitle
  if options.yMax or options.yMin then clip = {max = options.yMax, min = options.yMin} end
  if options.vtitle then vtitle = options.vtitle: gsub ('+',' ') end
  opt.vAxis = {title = vtitle, viewWindow = clip }
--  opt.crosshair = {trigger="selection", orientation = "vertical"}       -- or trigger = "focus"

  local chartType = "LineChart"
  if options.areaMode and (options.areaMode ~= "none") then chartType = "AreaChart" end
  chartType = options.graphType or chartType    -- specified value overrides defaults
  local cpu = daemon.cpu_clock ()
  local chart = gviz.Chart (chartType)
  local status = chart.draw (data, opt)
  cpu = daemon.cpu_clock () - cpu
  plot.cpu = plot.cpu + (cpu - cpu % 0.001)
  plot.mode = mode
  plot.nulls = nulls
  plot.total = plot.total + 1
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
  local data = {
    '[{',
    '  "target": "'..p.target..'",',
    '  "datapoints": ['
  }
  local n = tv.values.n or 0
  local nocomma = {[n] = ''}
  for i, v,t in tv:ipairs() do
    data[#data+1] = table.concat {'  [', v or 'null', ', ', t, ']', nocomma[i] or ','}
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

function _G.HTTP_DataGraphRenderer (_, ...)         -- renderer handler
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
  config.DATAGRAPH = {VERSION = ABOUT.VERSION, whisper = ROOT, stats = stats}
end 

----
