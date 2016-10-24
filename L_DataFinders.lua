module ("L_DataFinders", package.seeall)

ABOUT = {
  NAME            = "graphite_api.finders";
  VERSION         = "2016.10.04";
  DESCRIPTION     = "containing finders for: whisper, datamine";
  AUTHOR          = "@akbooer";
  COPYRIGHT       = "(c) 2013-2016 AKBooer";
  DOCUMENTATION   = "http://graphite-api.readthedocs.org/en/latest/",
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


---------------------------------------------------
--
-- Finders
--
-- the implementations for Whisper and dataMine are totally separate, 
-- but there is a lot of repetition (and some subtle differences.)
-- TODO: extract shared routines from finders
--

local graphite_api = require "L_DataGraphiteAPI"
local lfs = require "lfs"

-- for Whisper
local whisperDB = require "L_DataWhisper"               -- the Whisper database library

-- for dataMine 
local dmDB = require "L_DataBaseDM"
local library = require "L_DataLibrary"

local json = library.json()

local node = graphite_api.node 
local BranchNode, LeafNode = node.BranchNode, node.LeafNode

local utils = graphite_api.utils
local string_split, sorted = utils.string_split, utils.sorted
local expand_value_list = utils.expand_value_list

local intervals = graphite_api.intervals
local Interval = intervals.Interval
local IntervalSet = intervals.IntervalSet


---------------------------------------------------
--
-- Whisper finder
--

-- Reader is the class responsible for fetching the datapoints for the given path. 
-- It is a simple class with 2 methods: fetch() and get_intervals():

local function WhisperReader(fs_path, real_metric_path, carbonlink)

  -- find min and max times in tv array (interleaved times and values)
  -- note that a time of zero means, in fact, undefined
  local function min_max (x)
    local min,max = os.time(),x[1]
    for i = 1,#x, 2 do
      local t = x[i]
      if t > 0 then
        if t > max then max = t end
        if t < min then min = t end
      end
    end
    return min, max
  end  
  
  -- gets the timestamp of the oldest and newest datapoints in file
  local function earliest_latest (header)
    local archives = header.archives
    -- search for latest in youngest archive
    local youngest = archives[1].readall()
    local _, late = min_max (youngest)
--    early = os.time() - header['maxRetention']    -- instead, search for earliest in oldest archive
    local oldest = archives[#archives].readall()
    local early = min_max (oldest)
    if late < early then late = early end
    return Interval (early, late)
  end

-- get_intervals() is a method that hints graphite-web about the time range available for this given metric in the database. 
-- It must return an IntervalSet of one or more Interval objects.
  local function get_intervals()
    local start_end = whisperDB.__file_open(fs_path,'rb', earliest_latest)
    return IntervalSet {start_end}    -- TODO: all of the archives separately?
  end

-- fetch() returns a list of 2 elements: the time info for the data and the datapoints themselves. 
-- The time info is a list of 3 items: the start time of the datapoints (in unix time), 
-- the end time and the time step (in seconds) between the datapoints.
-- datapoints is a list of points found in the database for the required interval. 
-- There must be (end - start) / step points in the dataset even if the database has gaps: 
-- gaps are filled with 'nil' values.
  local function fetch(startTime, endTime)
--        logger.debug("fetch", reader="whisper", path=self.fs_path,
--                     metric_path=self.real_metric_path,
--                     start=startTime, end=endTime)
    
    -- carbon cache unused in DataYours

    return whisperDB.fetch(fs_path, startTime, endTime)   -- return whatever Whisper returns
  end


  return setmetatable ({
    fetch = fetch,
    get_intervals = get_intervals,
  },{
    __tostring = function () return "whisper reader" end   -- say what we are
  })
end


local function WhisperFinder(config)
  local self = {}
  self.directories = config['whisper']['directories']
  self.carbonlink = nil     -- not supporting carbon cache

  -- the DataYours implementation of the Whisper database is a single directory 
  -- with fully expanded metric path names as the filenames, eg: system.device.service.variable.wsp
  local function buildTree (name, dir)
    local a,b = name: match "^([^%.]+)%.(.*)$"      -- looking for a.b
    if a then 
      dir[a] = dir[a] or {}
      buildTree (b, dir[a])     -- branch
    else
      dir[name] = false         -- not a branch, but a leaf
    end
  end


  -- find_nodes() is the entry point when browsing the metrics tree.
  -- It is an iterator which yields leaf or branch nodes matching the query
  -- query is a FindQuery object. 
  local function find_nodes(query)
--    logger.debug("find_nodes", finder="whisper", start=query.startTime,
--                 end=query.endTime, pattern=query.pattern)
    local path_separator  = package.config:sub(1,1)
    local clean_pattern   = query.pattern: gsub ('\\', '')
    local pattern_parts   = string_split (clean_pattern, '.')

    for _,root_dir in ipairs (self.directories) do
      
      local dir = {}
      for a in lfs.dir (root_dir) do              -- scan the root directory and build tree of metrics
        local name = a: match "(%w.+)%.wsp$" 
        if name then buildTree(name ,dir) end
      end
      
      -- construct and yield an appropriate Node object
      local function yield_node_object (metric_path, branch)
        if branch then
          coroutine.yield (BranchNode(metric_path))
        else 
          local absolute_path = table.concat {root_dir, path_separator, metric_path, ".wsp"}
          local real_metric_path = metric_path        -- not supporting symbolic links
          local reader = WhisperReader(absolute_path, real_metric_path,
                                 self.carbonlink)
          coroutine.yield (LeafNode(metric_path, reader))
        end
      end
      
      --  Recursively generates absolute paths whose components
      --  underneath current_dir match the corresponding pattern in patterns
      local function _find_paths (current_dir, patterns, i, metric_path_parts)
        local qi = patterns[i]
        if qi then
          for qy in expand_value_list (qi) do     -- do value list substitutions {a,b, ...} 
            qy = qy: gsub ("[%-]", "%%%1")        -- quote special characters
            qy = qy: gsub ("%*", "%.%1")          -- precede asterisk wildcard with dot (converting regex to Lua pattern)
            qy = qy: gsub ("%?", ".")             -- replace single character query '?' with dot '.'
            qy = '^'.. qy ..'$'                   -- ensure pattern matches the whole string
            for node, branch in sorted (current_dir) do
              local ok = node: match (qy)
              if ok then
                metric_path_parts[i] = ok
                if i < #patterns then
                  if branch then
                    _find_paths (branch, patterns, i+1, metric_path_parts)
                  end
                else
                  local metric_path = table.concat (metric_path_parts, '.')
                  -- Now construct and yield an appropriate Node object            
                  yield_node_object (metric_path, branch)
                end
              end
            end
          end
        end
      end

      _find_paths (dir, pattern_parts, 1, {}) 

    end
  end

  -- WhisperFinder()
  return {
    find_nodes = function(query) 
      return coroutine.wrap (function () find_nodes (query) end)  -- a coroutine iterator
    end
  }
end

whisper = {WhisperFinder = WhisperFinder}

---------------------------------------------------
--
-- DataMine finder
--


-- Reader is the class responsible for fetching the datapoints for the given path. 
-- It is a simple class with 2 methods: fetch() and get_intervals():

local function DataMineReader(dm, metric_path)  
  local _, dev, srv, var = unpack (string_split (metric_path, '.'))
  dev = tonumber(dev)
  local dmc, status = dm:openCursor {key = {Service = srv, Variable = var, Device = dev} }
  if not dmc then return nil, status end
  
-- get_intervals() is a method that hints graphite-web about the time range available for this given metric in the database. 
-- It must return an IntervalSet of one or more Interval objects.
  local function get_intervals()
    local _, early = dmc.getFirst()
    local _, late  = dmc.getLast()
--    print ("GET INTERVALS", early, late)
    early = early or 0
    late = late or 1
    if late <= early then late = early + 1 end
    local early_late = Interval (early, late)
    return IntervalSet {early_late}    -- TODO: whole array
  end

-- fetch() returns a list of 2 elements: the time info for the data and the datapoints themselves. 
-- The time info is a list of 3 items: the start time of the datapoints (in unix time), 
-- the end time and the time step (in seconds) between the datapoints.
-- datapoints is a list of points found in the database for the required interval. 
-- There must be (end - start) / step points in the dataset even if the database has gaps: 
-- gaps are filled with 'nil' values.
-- NOTE: A significant departure from this model for dataMine, which has non-uniform sampling.
--        the time info step size is nil
--        the additional iterator is the way to navigate the data.
  local function fetch(startTime, endTime)
--        logger.debug("fetch", reader="whisper", path=self.fs_path,
--                     metric_path=self.real_metric_path,
--                     start=startTime, end=endTime)
--    local data = whisper.fetch(fs_path, startTime, endTime)
    
    local times = {t1 = startTime, t2 = endTime}
    local V,T = {}, {}
    for n, v,t in dmc:searchKeyRange (times) do
      V[n] = v; T[n] = t 
    end
    
    local n = #T
    V.n = n               -- add number of elements to array
                          -- (for dataMine, they're all non-zero, but not so for Whisper)
    local time_info = {T[1], T[n], nil}
    
    return 
      {        -- NOTE: the Whisper table structure as used in DataYours
        times  = time_info, 
        values = V,
        ipairs = function ()               
          local i,v,t = 0,V,T             -- make local for quicker access
          local function iterator ()
            i = i + 1
            if t[i] then return i, v[i],t[i] end
          end
          return iterator
        end
      }

  end


  return setmetatable ({
    fetch = fetch,
    get_intervals = get_intervals,
  },{
    __tostring = function () return "datamine reader" end   -- say what we are
  })
end



local function DataMineFinder(config)
  local self = {}
  self.directories = config['datamine']['directories']
  self.maxpoints   = config['datamine']['maxpoints']
  self.vera        = config['datamine']['vera']
  self.json        = config['datamine']['json']

  -- the dataMine database is a complex multi-file structure 
  -- but its primary metadata is in a single JSON file which includes information on variables. 
  
  local function buildTree (name, dir)
    local a,b = name: match "^([^%.]+)%.(.*)$"      -- looking for a.b
    if a then 
      dir[a] = dir[a] or {}
      buildTree (b, dir[a])     -- branch
    else
      dir[name] = false         -- not a branch, but a leaf
    end
  end


  -- find_nodes() is the entry point when browsing the metrics tree.
  -- It is an iterator which yields leaf or branch nodes matching the query
  -- query is a FindQuery object. 
  local function find_nodes(query)
--    logger.debug("find_nodes", finder="whisper", start=query.startTime,
--                 end=query.endTime, pattern=query.pattern)
    local path_separator  = package.config:sub(1,1)
    local clean_pattern   = query.pattern: gsub ('\\', '')
    local pattern_parts   = string_split (clean_pattern, '.')

    for _,root_dir in ipairs (self.directories) do
      if not root_dir: match ("%"..path_separator.."$") then
        root_dir = root_dir .. path_separator
      end
      local dm, dmDBstatus = dmDB.open {
        json = self.json or json, 
        database = root_dir, 
        maxpoints = tonumber (self.maxpoints) or 2000
      }
      local vars
      if dm then
        vars = dm.get {key = "Variables"}
      else 
        error (dmDBstatus, 2)
      end
      local dir = {}
      local fmt = (self.vera or "DM") .. ".%03d"
      for _,v in pairs (vars or {}) do              -- scan the root directory and build tree of metrics
        local name = table.concat ({fmt:format (v.Device), v.Service, v.Variable}, '.')
        if name then buildTree(name, dir) end
      end
      
      -- construct and yield an appropriate Node object
      local function yield_node_object (metric_path, branch)
        if branch then
          coroutine.yield (BranchNode(metric_path))
        else 
          local reader = DataMineReader(dm, metric_path)
          if reader then coroutine.yield (LeafNode(metric_path, reader)) end
        end
      end
      
      --  Recursively generates absolute paths whose components
      --  underneath current_dir match the corresponding pattern in patterns
      local function _find_paths (current_dir, patterns, i, metric_path_parts)
        local qi = patterns[i]
        if qi then
          for qy in expand_value_list (qi) do     -- do value list substitutions {a,b, ...} 
            qy = qy: gsub ("[%-]", "%%%1")        -- quote special characters
            qy = qy: gsub ("%*", "%.%1")          -- precede asterisk wildcard with dot
            qy = qy: gsub ("%?", ".")             -- replace single character query '?' with dot '.'
            qy = '^'.. qy ..'$'                   -- ensure pattern matches the whole string
            for node, branch in sorted (current_dir) do
              local ok = node: match (qy)
              if ok then
                metric_path_parts[i] = ok
                if i < #patterns then
                  if branch then
                    _find_paths (branch, patterns, i+1, metric_path_parts)
                  end
                else
                  local metric_path = table.concat (metric_path_parts, '.')
                  -- Now construct and yield an appropriate Node object            
                  yield_node_object (metric_path, branch)
                end
              end
            end
          end
        end
      end

      _find_paths (dir, pattern_parts, 1, {}) 

    end
  end

  -- DataMineFinder()
  return {
    find_nodes = function(query) 
      return coroutine.wrap (function () find_nodes (query) end)  -- a coroutine iterator
    end
  }
end

datamine = {DataMineFinder = DataMineFinder}

----

-- TEST

--local config = {
--  whisper = {
--    directories = {"/Volumes/DataMine/whisper"} --/Edge", "/Volumes/DataMine/whisper/MiOS"}
--  }
--}

--local finder = WhisperFinder (config)
--local pretty = require "pretty"

--local N = 0
--local now = os.time()

--for n in finder.find_nodes {pattern = "*.*.*Temp*.*"} do
----for n in finder.find_nodes {pattern = "*.*"} do
--  N = N + 1
--  print (n)
--  if n.is_leaf then
--    local x = n.fetch (now-3600)
--    local t = x.times
--    print ('', pretty (t))
--    print ('', os.date ("%c",t[1]), os.date ("%c",t[2]))
--    print ('', "#points:", x.values.n)

--    for i,v,t in x:ipairs()  do
--      print (i,t,v)
--      if i >= 10 then break end
--    end
--  end
--end


--print ("number of nodes",N)


