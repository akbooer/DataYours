module ("L_DataCache", package.seeall)

local ABOUT = {
  NAME            = "DataCache";
  VERSION         = "2016.10.04";
  DESCRIPTION     = "DataCache - Carbon Cache daemon";
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
-- DataCache: data collection back-end using Whisper
-- 
-- DataCache mimics a Carbon "cache" daemon, saving incoming data to a Whisper database.
-- reads the Graphite format "storage-schemas.conf" and "storage-aggregation.conf" files.
-- also "rewrite-rules.conf" for that functionality of carbon-aggregator.
--

local DataDaemon = require "L_DataDaemon"
local whisper    = require "L_DataWhisper"


local daemon                    -- the daemon object with useful methods
local syslog                    -- syslog socket for logging data
local listen                    -- listen object (with close method, if necessary)
local config                    -- all our configuration (and the .conf file info)
local cache                     -- our section of the carbon.conf file 
local ROOT                      -- for the whisper database
local filePresent = {}          -- cache of existing file names

local rules                     -- rewrite rules
local tally = {n = 0}
local stats = {                 -- interesting performance stats
    cpu = 0,
    updates = 0,
  }

local default = {
  schema  = {name = "[default]",  retentions = "1h:7d"},                                    -- default to once an hour for a week
  aggregation = {name = "[default]", xFilesFactor = 0.5, aggregationMethod = "average" },   -- these are the usual Whisper defaults anyway
}

----
--
-- Storage Schemas and aggregation configuration rules
-- 
-- for whisper.create(path, archives, xFilesFactor, aggregationMethod)
-- are read from two Graphite-format .conf files: storage-schemas.conf and storage-aggregation.conf
-- both STORED IN THE WHISPER ROOT DIRECTORY (because the files relate to storage capacity)
-- see: http://graphite.readthedocs.org/en/latest/config-carbon.html#storage-schemas-conf
-- and: http://graphite.readthedocs.org/en/latest/config-carbon.html#storage-aggregation-conf

local function match_rule (item, rules)
  -- return rule for which first rule.pattern matches item
  for _,rule in ipairs (rules) do
    if rule.pattern and item: match (rule.pattern) then return rule end
  end
end
   
--[[  

from: http://graphite.readthedocs.org/en/latest/config-carbon.html#rewrite-rules-conf

Rewrite rules allow you to rewrite metric names using regular expressions. Note that unlike some other config files, any time this file is modified it will take effect automatically. This requires the carbon-aggregator service to be running.

The form of each line in this file should be as follows:
regex-pattern = replacement-text

This will capture any received metrics that match ‘regex-pattern’ and rewrite the matched portion of the text with ‘replacement-text’. The ‘regex-pattern’ must be a valid regular expression, and the ‘replacement-text’ can be any value. You may also use capture groups.

rewrite-rules.conf consists of two sections, [pre] and [post]. The rules in the pre section are applied to metric names as soon as they are received. The post rules are applied after aggregation has taken place.

]]

local pre, post     -- rewrite rules go here

local function load_rewrite_rules (path)
  local rules = {bin = {}}
  local r = rules.bin
  local f = io.open (path)
  if f then 
    for l in f:lines() do
      if not l: match "^%s*#" then                -- ignore comment lines
        l = l: gsub ("[\001-\031]",'')            -- remove any control characters (just in case)
        local tag = l: match"^%s*%[(%w+)%]"
        if tag then                               -- set up tag in rules
          rules[tag] = rules[tag] or {}
          r = rules[tag]
        else
          local p, q = l: match "^%s*(%S+)%s*=%s*(%S*)"  -- pattern = replacement
          if q then
            p = p: gsub ("\\",'%%')        -- convert from regular expression to Lua pattern
            q = q: gsub ("\\",'%%') 
            r[#r+1] = {pattern = p, replacement = q}
          end
        end
      end
    end
    f: close ()
  end
  return rules.pre, rules.post
end

-- there's no aggregation stage (yet), but [pre] and [post] rules allow two passes of rewriting

local function apply_rewrite (x, rules)
  for _, rule in ipairs (rules or {}) do
    local new, n = x: gsub (rule.pattern, rule.replacement, 1)
    if n > 0 then x = new break end
  end
  return x
end

----
--
-- UDP callback (for incoming data)
--
-- Whisper file update - could make this much more complex 
-- with queuing and caching like CarbonCache, but let's not yet.
-- message is in Whisper plaintext format: "path value timestamp"
-- 

local function UDPhandler (msg, ip) -- update whisper file, creating new file if necessary
  local _ = ip      -- unused at present
  local filename, path, value, timestamp
  local function create () 
    local logMessage1 = "created: %s"
    local logMessage2 = "schema %s = %s, aggregation %s = %s, xff = %.0f"
    local rulesMessage   = "rules: #schema: %d, #aggregation: %d"
    if not whisper.info (filename) then   -- it's not there
      -- load the rule base (do this every create to make sure we have the latest)
      local schemas     = DataDaemon.read_conf_file (ROOT .. "storage-schemas.conf")
      local aggregation = DataDaemon.read_conf_file (ROOT .. "storage-aggregation.conf")      
      daemon.log (rulesMessage: format (#schemas, #aggregation) )   
      -- apply the matching rules
      local schema = match_rule (path, schemas)     or default.schema
      local aggr   = match_rule (path, aggregation) or default.aggregation
      whisper.create (filename, schema.retentions, aggr.xFilesFactor, aggr.aggregationMethod)  
      daemon.log (logMessage1: format (path or '?') )
      daemon.log (logMessage2: format (schema.name, schema.retentions, aggr.name,
                     aggr.aggregationMethod or default.aggregation.aggregationMethod, 
                     aggr.xFilesFactor or default.aggregation.xFilesFactor) )
    end
    filePresent[filename] = true
  end
  -- update ()
  path, value, timestamp = msg: match "([^%s]+)%s+([^%s]+)%s*([^%s]*)"
  if path and value then
    if timestamp == '' then timestamp = os.time() end     -- add local time if sender has no timestamp
    if pre or post then
      local new = path
      new = apply_rewrite(new, pre)
      -- carbon-aggregator code would go here
      new = apply_rewrite(new, post)
      if new ~= path then
        rules.rewrites[new] = path
        path = new
      end
    end
    filename = table.concat {ROOT, path:gsub(':', '^'), ".wsp"}    -- change ":" to "^" and add extension 
    timestamp = tonumber (timestamp)   
    value = tonumber (value)
    if not filePresent[filename] then create () end         -- we may need to create it
    local cpu = daemon.cpu_clock ()
    -- use remote timestamp as time 'now' to avoid clock sync problem of writing at a future time
    whisper.update (filename, value, timestamp, timestamp)  
    cpu = stats.cpu + (daemon.cpu_clock () - cpu)
    stats.cpu = cpu - cpu % 0.001
    stats.updates = stats.updates + 1
    if not tally[path] then
      tally[path] = 0
      tally.n = tally.n + 1
    end
    tally[path] = tally[path] + 1
    if cache.LOG_UPDATES then syslog: send (msg) end
  end
end

-- Initialisation

function init ()
  daemon = DataDaemon.start {Name = "DataCache", UDP_callback = UDPhandler}
  config = daemon.config
  cache = config["[cache]"] or {}
     
  ROOT = cache.LOCAL_DATA_DIR     -- where to look for a database
  pre, post = load_rewrite_rules (ROOT .. "rewrite-rules.conf")
  rules = {pre = pre, post = post, rewrites = {}}
  config.DATACACHE = {
    VERSION = ABOUT.VERSION, 
    whisper = ROOT, 
    stats = stats, 
    tally = tally, 
    rules = rules,
  }
  
  syslog = daemon.open_for_syslog (cache.SYSLOG) 
  listen = daemon.open_for_listen (cache.UDP_RECEIVER_PORT, UDPhandler) 
end

----
