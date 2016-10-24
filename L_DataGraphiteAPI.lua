module ("L_DataGraphiteAPI", package.seeall)

ABOUT = {
  NAME            = "graphite_api";
  VERSION         = "2016.10.04";
  DESCRIPTION     = "containing: utils, nodes, intervals, storage";
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

--[[

Graphite_API 

  "Graphite-web, without the interface. Just the rendering HTTP API.
   This is a minimalistic API server that replicates the behavior of Graphite-web."

see:
  https://github.com/brutasse/graphite-api
  https://github.com/brutasse/graphite-api/blob/master/README.rst

with great documentation at:
  http://graphite-api.readthedocs.org/en/latest/
  
  "Graphite-API is an alternative to Graphite-web, without any built-in dashboard. 
   Its role is solely to fetch metrics from a time-series database (whisper, cyanite, etc.)
   and rendering graphs or JSON data out of these time series. 
   
   It is meant to be consumed by any of the numerous Graphite dashboard applications."
   

Originally written in Python, I've converted some parts of it into Lua with slight tweaks 
to interface to the DataYours implementation of Carbon / Graphite.  
This is why it sometimes looks a little strange for Lua code.

It provides sophisticated searches of the database and the opportunity to link to additional databases.
I've written a finder specifically for the dataMine database, to replace the existing dmDB server.

@akbooer,  February 2016

--]]


---------------------------------------------------
--
-- Utilities
--


-- split character string into parts divided by single character
local function string_split (self, sep) 
	local x = {}
  local quote = {['.'] = "%."}                                  -- quote special characters
  sep = (quote[sep] or sep or ' ')
	local pattern = "([^" .. sep .. "]+)"
	self:gsub(pattern, function(c) x[#x+1] = c end)
	return x
end

-- sorted version of the pairs iterator
-- use like this:  for a,b in sorted (x, fct) do ... end
-- optional second parameter is sort function cf. table.sort
local function sorted (x, fct)
  local y, i = {}, 0
  for z in pairs(x) do y[#y+1] = z end
  table.sort (y, fct) 
  return function ()
    i = i + 1
    local z = y[i]
    return z, x[z]  -- if z is nil, then x[z] is nil, and loop terminates
  end
end

-- iterator to expand a series list string "preamble{A,B,C}postamble{X,Y,Z}" 
-- into a list of individual series "preambleApostambleX", etc.
local function expand_value_list (x)
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

--[[
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
--]]

utils = {
  expand_value_list = expand_value_list,
  sorted = sorted,
  string_split = string_split,
}

---------------------------------------------------
--
-- Nodes
--

--node = {}

local function Node (path)
  local self = {
      path = path,
      name = path: match "([^%.]+)%.?$" or '',
      ["local"] = true,
      is_leaf = false,
    }

  local meta = {
      class_name = "Node",
      id = tostring(self): match "[x%x]+$" or '?',
    }
    
  function meta.__tostring ()
      return ('<%s[%s]: %s>'): format (meta.class_name, meta.id, self.path)
    end
  
  return setmetatable (self, meta)
end

local function BranchNode (path)
  local self = Node (path)
  local meta = getmetatable (self)
  meta.class_name = "BranchNode"
  return self
end

-- LeafNode is created with a reader, which is the class responsible for fetching the datapoints for the given path. 
-- It is a simple class with 2 methods: fetch() and get_intervals()
local function LeafNode (path, reader)
  local self = Node (path)
  local meta = getmetatable (self)
  meta.class_name = "LeafNode"
  
--  self.intervals = reader.get_intervals()     -- see below...
  self.reader = reader
  self.is_leaf = true
  self.fetch = function (startTime, endTime)
    return self.reader.fetch(startTime, endTime)
  end
  
  -- reader.get_intervals() generally involves opening files and searching for timestamps,
  -- however, not every leaf node will necessarily have its intervals used by the caller,
  -- so provide lazy evaluation to speed up initialisaton, and evaluate and store only if accessed.
  function meta:__index (name)
    if name == "intervals" then
      local intervals = reader.get_intervals()
      rawset (self, name, intervals)
      return intervals
    end
  end
   
  return self
end


node = {
  BranchNode = BranchNode,
  LeafNode = LeafNode,
}

---------------------------------------------------
--
-- Intervals
--


local function IntervalSet (intervals)
  
  local function __repr__ (self)
    local t = {}
    for _, x in ipairs (self) do
      t[#t+1] = tostring(x)
    end
    return table.concat {'{', table.concat (t,','), '}'}
  end
  
  local meta = {__tostring = __repr__}

  return setmetatable (intervals, meta)
end



local function Interval(start, finish)

  assert (start <= finish, "Invalid interval: finish < start")

  local self = {}
  self.start = start
  self.finish = finish
  self.tuple = {start, finish}
  self.size = self.finish - self.start

  local function __eq__ (self, other)
    return (self.start == other.start) and (self.finish == other.finish)
  end

  local function __lt__(self, other)
    return (self.start < other.start) -- - (self.start > other.start) ???
  end
  
  local function __repr__(self)
    return ('<Interval: %s, %s>'): format (os.date("%d-%b-%Y",self.start), os.date("%d-%b-%Y",self.finish))
  end
  
  local function intersect(other)
      local start  = math.max(self.start,  other.start)
      local finish = math.min(self.finish, other.finish)

      if finish > start then
          return Interval(start, finish)
      end
  end

  local function overlaps(other)
    local earlier, later = self, other
    if self.start > other.start then 
      earlier, later = other, self 
    end
    return earlier.finish >= later.start
  end

  local function union(other)
    assert(self.overlaps(other) , "Union of disjoint intervals is not an interval")

    local start  = math.min(self.start, other.start)
    local finish = math.max(self.finish, other.finish)
    return Interval(start, finish)
  end
  
  local meta = {
    __eg = __eq__,
    __lt = __lt__,
    __tostring = __repr__,
    __index = {
      intersect = intersect,
      overlaps = overlaps,
      union = union,
    },
  }
  
  return setmetatable (self, meta)
end


intervals = {
  Interval = Interval,
  IntervalSet = IntervalSet,
}


---------------------------------------------------
--
-- Storage
--


local function FindQuery (pattern, startTime, endTime)
  
  local self = {}
  self.pattern = pattern
  self.startTime = startTime
  self.endTime = endTime
  self.isExact = true     -- is_pattern(pattern)
  self.interval = {startTime or 0, endTime or os.time()}   -- Interval()
  
  local meta = {}
  function meta.__tostring ()
    local startString, endString
    if not self.startTime then
      startString = '*'
    else
      startString = os.date ("%c",self.startTime)
    end

    if not self.endTime then
      endString = '*'
    else
      endString = os.date ("%c",self.endTime)
    end

    return ('<FindQuery: %s from %s until %s>'): format (self.pattern, startString, endString)
  end
  
  return setmetatable (self, meta)
end


local function Store (finders)
  
  local function find (pattern, startTime, endTime, islocal)
    if islocal == nil then islocal = true end     -- NB: can't use islocal = islocal or true here! (nil is not false here)
    local query = FindQuery(pattern, startTime, endTime)
        
    local matching_nodes = {}   --  set()
    
    -- Search locally
    for _,finder in ipairs (finders) do
      for node in finder.find_nodes(query) do
        table.insert (matching_nodes,node)
      end
    end
    
    -- Group matching nodes by their path
    local nodes_by_path = {}
    for _, node in ipairs (matching_nodes) do
      nodes_by_path[node.path] = nodes_by_path[node.path] or {}
      table.insert (nodes_by_path[node.path], node)
    end
    
    -- Reduce matching nodes for each path to a minimal set
    local found_branch_nodes = {}                 -- set

    for path, nodes in sorted (nodes_by_path) do
      local leaf_nodes = {}                       -- set
      
      -- First we dispense with the BranchNodes
      for _, node in ipairs (nodes) do
        if node.is_leaf then
          leaf_nodes[node] = true                 -- add 
        elseif not found_branch_nodes[node.path] then
          -- TODO: need to filter branch nodes based on requested interval... how?!?!?
          coroutine.yield (node)
          found_branch_nodes[node.path] = true    -- add
        end
      end

      local n_leaf = 0
      for _ in pairs (leaf_nodes) do n_leaf = n_leaf + 1 end
      
      if n_leaf == 1 then
        coroutine.yield (next(leaf_nodes))      -- currently, only return the FIRST leaf node
      elseif n_leaf > 1 then
        local reader = MultiReader(leaf_nodes)    -- TODO: MultiReader not yet implemented
        coroutine.yield (LeafNode(path, reader))
      end
    end
  end

  -- Store()
  return {find =     -- find is an iterator which yields nodes
      function(x) 
        return coroutine.wrap (function() find(x) end)
      end
    }
  
end


storage = {
  FindQuery = FindQuery,
  Store = Store,
}

-----

