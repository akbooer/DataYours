module ("L_DataUser", package.seeall)
 
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
-- 2016.04.08   @akbooer
--
-- DataUser is a user-defined module with a single global function 'run' 
-- called by DataWatcher for every incoming metric (wherever it comes from.)
-- The processing can do anything you like within the Luup environment
-- and returns a single function, an iterator, which returns the
-- (possibly modified) metric and data to send to the DataCache for storage.
--
-- The iterator function is called until its first return argument is nil.
-- This module can, therefore, choose to totally reject an incoming metric or 
-- return multiple different ones for storage.
--
-- By default, this module simply returns each metric unchanged.
-- 

function run (metric, value, time)  
  
  local function relay ()
    local m = metric
    metric = nil
    return m, value, time   -- first call returns metric, second one nil
  end
  
  -----
  --
  -- user-defined processing goes here
  -- change metric name, value, or time as required, or
  -- set metric to nil if you don't want to pass it on
  --
  -- Typically, your processing will test for a specific metric
  -- and modify the value under certain conditions.  
  -- A simple example is bounds-checking:
  --
  --  if metric: match "Temperature" then 
  --    local v = tonumber(value)
  --    if v < -50 then value = -50 
  --    elseif v > 150 then value = 150
  --    end
  --  end
  --
  -----
  
  return relay
end

-----

--TEST

--for a,b,c in run ("whisper.Temperature.metric.name", 42, os.time()) do
--  print (a,b,c)
--end
