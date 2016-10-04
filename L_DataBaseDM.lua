module ("L_DataBaseDM", package.seeall)

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
-- (c) 2013-2016, AK Booer
--
-- dmDB, a simple read-only wrapper for the dataMine database.
-- database access semantics are loosely modelled on the "Berkeley DB" Core API methods
-- see: http://www.oracle.com/technetwork/products/berkeleydb/documentation/index.html
-- and: http://forum.micasaverde.com/index.php/topic,17499.0.html
--

-- 2013.12.28  baseline
-- 2016/02/07  Use LuaFileSystem to scan for weekly files rather than opening each one: SO much faster

local lfs = require "lfs"


local function method () error ("undeclared interface element", 2) end
local function interface (i) return setmetatable (i, {__newindex = method}) end


--local dmDB = interface {
  -- class methods
  open              = method;
  search            = method;
  -- info
  _AUTHOR           = "@akbooer";
  _COPYRIGHT        = "(c) 2013-2015 AKBooer";
  _NAME             = "DataBaseDM";
  _VERSION          = "2016.02.07";
  _DESCRIPTION      = "a simple read-only wrapper for the dataMine database";
--}

--
-- example call sequence: (you don't need all of this to get useful info from the DB, this just documents the calls)
-- dmDB = require "dmDB"            -- load the code
-- dm = dmDB.open {}              -- open the database
-- dm:get {}                  -- get the configuration for the whole database (decoded from dataMineConfig.json file)
-- dm:get {key = "Variables"}         -- get the named field of the above config
-- dmc = dm:openCursor {key = {Id = 42} }   -- open a cursor on a channel by its dataMine Id number
-- dmc = dm:openCursor {key = {Name = "foo"} }  -- open a cursor on a channel by its dataMine channel name
-- dmc = dm:openCursor {key = {Service = srv, Variable = var, Device = dev} } -- open a channel by its Luup coordinates
-- dmc:get {}                 -- get the configuration for this channel (decoded from channel's config.json file) 
-- dmc:get {key = "Id"}             -- get the named field of the above channel config
-- dmc:getFirst ()                -- get time/value pairs using the cursor, also Last, Next, Prev, Current
-- dmc:getSearchKeyRange {t = T}        -- get the first time/value pair at the time >= T
-- dmc:getSearchKeyRange {t1 = a, t2 = b}   -- get time/value pairs in interval t1 <= range < t2
-- dmc:close ()                 -- close the cursor
-- dm:close ()                  -- close the database
--


local default = {                         -- parameter defaults
    json = "json-dm",                     -- not great, but guaranteed to be there!
    database  = "/dataMine/",                 -- database filesystem root
    earliest  = os.time { day = 1, month = 1, year = 2012 },  -- earliest allowable data retrieval
    maxpoints = 10080,                      -- maximum number of points returned by keyRange
  }


-- Class methods

-- utility search methods

function search (tables, match)       -- search fields in array of tables which match given (multiple, possibly partial) criteria
  local copy = {}
  for i,j in pairs (tables) do copy[i] = j end
  for name,value in pairs (match) do
    if type (value) == "string" then      -- allow partial matches for strings
      for n, item in pairs  (copy) do
        if not string.find(item[name], value,nil,true) then copy[n] = nil end
      end
    else                    -- otherwise exact match required
      for n, item in pairs  (copy) do
        if item[name] ~= value then copy[n] = nil end
      end
    end
  end
  local matching = {}
  for _,j in pairs (copy) do matching[#matching+1] = j end
  return matching   -- never nil, but possibly empty
end

  
function locate (x, Xs) -- return location of first element in (sorted) Xs >= x using bisection , or nil if none
  local function bisect (a,b)
    if a >= b then return a end
    local c = math.floor ((a+b)/2)
    if x <= Xs[c]
      then return bisect (a,c)
      else return bisect (c+1,b)
    end
  end 
  local n = #Xs
  if n == 0 or Xs[n] < x
    then return nil 
    else return bisect (1, n) 
  end
end


local function mapFile (filename, fct, ...)     -- opens filename and passes file handle to fct for processing, returning any parameters
  local returns
  local unpack = unpack or table.unpack     -- difference between Lua v5.1 and v5.2 ?
  local f = io.open (filename, 'r')
  if f then 
    returns = { fct(f, ...) }           -- pass file handle and additional parameters
    f: close()
  end
  return unpack (returns or {nil})
end

  
-----
--
-- dmDB object method
-- 

function open (self, flags)

  flags = flags or self or {}
  
  local DB = {
    database  = flags.database  or default.database,        -- filesystem where the dataMine db resides
    earliest  = flags.earliest  or default.earliest,        -- possibly supplied in flags: {earliest = os.time (...)}
    maxpoints = flags.maxpoints or default.maxpoints,       -- max samples returned
    json    = flags.json      or default.json,          -- JSON module filename
    index   = nil                         -- index to variable names / channels / etc...
  }

  local json = DB.json    -- might be actual module
  if type(json) == "string" then json = require (DB.json) end
  if type(json) ~= "table" then return nil, ("json library '%s' not found"): format (DB.json) end

  --
  -- utility routines
  --
  
  local function weeknum (time)           -- returns week number of given time (or now)
    local weekSeconds = 7 * 24 * 60 * 60
      return math.floor((time or os.time() ) / weekSeconds)
  end
    
  local function getDBconfig (subset, n)    -- returns decoded config file (or nil if database not found) 
    local filename, status
    if n then filename = ("%sdatabase/%s/config.json"): format (DB.database, n)   -- specific channel n, or...
       else filename = ("%sdataMineConfig.json"): format (DB.database)      -- ...whole database
    end
    local t = os.clock()
    local txt = mapFile (filename, function (f) return f:read '*a' end)
    local info = json.decode(txt or "") 
    t = (os.clock() - t) * 1e3
    if info then
      if subset then info = info[subset] end                -- pick a subset
      status = ("dataMine configuration file at '%s' opened OK, CPU = %.3f mS"): format (filename, t)
    else
      status = ("unable to open dataMine configuration file '%s'"): format (filename)
    end
    return info, status   
  end
  
  -----------------------
  --
  -- Cursor object methods
  -- 
  
  -- @param:key is the dataMine channel Id to access
  local function openCursor (self, flags)   

    flags = flags or self or {}       -- colon or dot notation
  
    local weeks = {}            -- list of the available weekly files
    local cursor              -- current cursor position, index into times/values array
    local channelId             -- dataMine channel ID for data file

    local getError = 'error in cursor get'

    local status                -- status or error message used throughout
    local cachedWeek
    local cachedTimes, cachedValues = {}, {}  -- cached week info 
  
    local function weekFilename (weekNo)    -- return filename n for weekNo
      return ("%sdatabase/%s/raw/%s.txt"): format (DB.database, channelId, weekNo)
    end

--    local function scanWeeks ()         -- initialise list of available weekly files
--      for wn = weeknum (DB.earliest), weeknum ()  do        -- from then until now
--        mapFile (weekFilename(wn), function (_, wn) weeks [#weeks+1] = wn end, wn)
--      end
--      return #weeks
--    end

    local function scanWeeks ()         -- initialise list of available weekly files
      local dir = ("%sdatabase/%s/raw"): format (DB.database, channelId)
      for file in lfs.dir (dir) do
        local wn = file: match "^(%d+)%.txt$" 
        wn = tonumber (wn)
        if wn then weeks [#weeks+1] = wn end
      end
      return #weeks
    end
  
    local function readWeek (n, t, v)     -- append a week's worth of data to t and v
      local info = mapFile (weekFilename(weeks[n]), function (f) return f:read '*a'end)
      local n = #t
      for time, value in info: gmatch "([^,]+),([^\n]+)" do
        n = n + 1
        t[n] = tonumber (time)
        v[n] = tonumber (value)
--        t[n] = time
--        v[n] = value
      end 
    end

--    local function readWeek (n, t, v)     -- append a week's worth of data to t and v
--      local f = io.open (weekFilename(weeks[n])) 
--      if f then
--        local n = #t
--        repeat
--          n = n + 1
--          t[n] = f:read '*n'
--          f:read (1)
--          v[n] = f:read '*n'
--        until not v[n]
--        f:close () 
--      end 
--    end
          
    local function getWeek (n)
      status = getError
      cachedWeek = nil                  -- flush the cache
      cachedTimes, cachedValues = {}, {}
      if 0 < n and n <= #weeks then
        cachedWeek = n
        status = ("cursor in week #%d"): format (weeks[n])
        readWeek (n, cachedTimes, cachedValues)
      end
    end 

    local function getCurrent ()
      return cachedValues[cursor], cachedTimes[cursor], status
    end
    
    local function getFirst ()
      getWeek (1)
      cursor = 1
      return getCurrent ()
    end
    
    local function getLast ()
      getWeek (#weeks)
      cursor = #cachedTimes
      return getCurrent ()
    end
    
    local function getNext ()
      if not cursor then return getFirst () end -- so you can traverse the data just calling getNext() until nil
      if cursor == #cachedTimes then
        getWeek (cachedWeek + 1)
        cursor = 0
      end
      cursor = cursor + 1
      return getCurrent ()
    end
    
    local function getPrev ()
      if not cursor then return getLast () end  -- so you can traverse the data (backwards) just calling getPrev() until nil
      if cursor == 1 then
        getWeek (cachedWeek - 1)    -- TODO: suppose there is one missing ???
        cursor = #cachedTimes + 1
      end
      cursor = cursor - 1
      return getCurrent ()
    end
    
    local function setCursor (t)                -- move cursor to first time >= t
      local wf = locate (weeknum (t), weeks)          -- do we have that week? ...or a following one?
      local ok = false
      if wf then 
        if wf ~= cachedWeek then 
          getWeek (wf) 
        end
        cursor = locate (t, cachedTimes)          -- do we have that time?... or a following one?
        if not cursor then
          getWeek (cachedWeek + 1)            -- must be into next week
          cursor = 1
        end
        ok = cachedTimes[cursor]
      end
      return ok   
    end

    local function iterator (t2, n)
      n = n + 1
      local v,t = getCurrent () 
      if n > DB.maxpoints or not t or t >= t2 then return end
      if cursor == #cachedTimes then
        getWeek (cachedWeek + 1)
        cursor = 0
      end
      cursor = cursor + 1
      return n, v,t   
    end
    
    -- searchKeyRange(), iterator function returning count (n) and single value/time pair (v,y) sequentially over given range
    local function searchKeyRange (self, range)
      range = range or self or {}       -- dot or colon notation
      local t1 = range.t1 or DB.earliest
      local t2 = range.t2 or os.time ()
      local ok = setCursor (t1) 
      if not ok then t2 = 0 end       -- finish before we start!
      return iterator, t2, 0 
    end

    local function getSearchKeyRange (self, range)    -- return data array from t1 <= range < t2, or just single point t
      range = range or self or {}       -- dot or colon notation
      if range.t then 
        local ok = setCursor (range.t)  
        if ok then return getCurrent() else return nil, nil, getError end     -- only one point from {t = T} request
      else
        local vs,ts = {}, {}
        for n, v,t in searchKeyRange (range) do
          vs[n] = v
          ts[n] = t 
        end
        local status = ("number of points in range = %d"): format (#ts)
        return vs,ts, status
      end
    end
            
    -- get (), returns channel config file contents in total, or named part (eg. get = {key = "Id"}, which is the dataMine channel Id)
    local function getCursor (self, flags)    
      flags = flags or self or {}             -- allow colon ':' or dot '.' calling syntax
      return getDBconfig (flags.key, channelId)
    end
    
    local function closeCursor ()             -- not required
      channelId = nil
    end
    
    -- openCursor()
    local key = flags.key or {}
    channelId = key.Id                    -- look directly for channel Id to open
    if not channelId then                 -- otherwise do index lookup with given info
      local found = search (DB.index, key)      
 -- TODO:     if #found ~= 1 then return nil, ("cursor key not found or not unique: %d matches"): format (#found) end
      channelId = found[1].Id
    end 
    
    if scanWeeks () == 0 then return nil, ("error opening raw data file for channel #%s"): format (channelId) end
    
    return {
          get = getCursor, getCurrent = getCurrent, 
          getSearchKeyRange = getSearchKeyRange, searchKeyRange = searchKeyRange,
          getFirst = getFirst, getLast = getLast, 
          getNext = getNext, getPrev = getPrev, 
          close = closeCursor ,
        }, 
          ("raw data file(s) for channel #%s opened OK, #weeks = %d"): format (tostring (channelId), #weeks), weeks 
      end
  

  -----------------------
  --
  -- dmDB object methods
  -- 
    
  -- get (), returns config file contents in total, or named part (eg. "Variables", which is the index into the raw data files)
  local function getDB (self, flags)
    flags = flags or self or {}               -- allow colon ':' or dot '.' calling syntax
    return getDBconfig (flags.key)              -- return required subset (or all) of config file
  end

  -- close (), entirely cosmetic
  local function closeDB ()
    DB = nil
  end
  
  -- openDB (database)
  local status
  DB.index, status = getDBconfig "Variables"        -- save the Variable index
  if not DB.index then return nil, status end
  return {get = getDB, 
      openCursor = openCursor, 
      close = closeDB }, status

end

----

