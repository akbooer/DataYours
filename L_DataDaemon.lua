module ("L_DataDaemon", package.seeall)

------------------------------------------------------------------------
--
-- DataDaemon: a generic framework for Graphite Carbon daemons
-- HTML is for configuration and UDP is for data (in and/or out)
-- used by: DataWatcher, DataCache, DataGraph, ...
-- UDP datagrams are all in Whisper plaintext format
--

local socket  = require "socket"


local function method () error ("undeclared interface element", 2) end
local function interface (i) return setmetatable (i, {__newindex = method}) end


--local DataDaemon = interface {
  -- constants  
  HOST = "Vera-" .. luup.pk_accesspoint;         -- our own hostname for syslog
  -- variables
  config_path     = "/www/";                    -- config file path
  -- methods
  cpu_clock       = method;                     -- CPU clock with wrap-around correction
  read_conf_file  = method;                     -- Carbon/Graphite config file reader
  pretty          = method;                     -- Lua pretty-printer
  set_config      = method;                     -- override config file reading
  start           = method;                     -- start a new daemon
  -- info
  _AUTHOR       = "@akbooer";
  _COPYRIGHT    = "(c) 2013-2016 AKBooer";
  _VERSION      = "2016.01.25";
  _DESCRIPTION  = "DataDaemon module for DataYours / Carbon daemons";
--}

local carbon_conf = {}                          -- global carbon configuration

------------------------------------------------------------------------
-- 
-- table = start(info), create a new daemon
-- info = {Name, UDP_callback, HTTP_callback, }
-- table = {syslog = syslog, send = send}     -- Config is table of persistent name/value text pairs
--  
-- HTTP server is http://127.0.0.1:3480/data_request?id=lr_<Name> followed by...
-- HTTP requests:
--  &show=config                        lists current internal configuration
--  &anythingElse = something           passed to client HTML callback
-- 



-- UTILITY FUNCTIONS


-- for external IP, instead, see: http://forum.micasaverde.com/index.php/topic,25621.msg181046.html#msg181046
-- with thanks to @hek for http://forum.micasaverde.com/index.php/topic,8505.msg93464.html#msg93464
-- and @guessed for http://forum.micasaverde.com/index.php/topic,23174.msg156990.html#msg156990
-- this version from: http://forums.coronalabs.com/topic/21105-found-undocumented-way-to-get-your-devices-ip-address-from-lua-socket/
local function myIP ()    
  local mySocket = socket.udp ()
  mySocket:setpeername ("42.42.42.42", "424242")  -- random IP and PORT
  local ip = mySocket:getsockname () 
  mySocket: close()
  return ip or "127.0.0.1"
end

 
--
-- CPU clock()
--
-- The system call os.clock() is a 32-bit integer which is incremented every microsecond 
-- and so overflows for long-running programs.  So need to count each wrap-around.
-- The reset value may return to 0 or -22147.483648, depending on the operating system

local  prev    = 0            -- previous cpu usage
local  offset  = 0            -- calculated value
local  click   = 2^31 * 1e-6  -- overflow increment

function cpu_clock ()
  local this = os.clock ()
  if this < prev then 
    offset = offset + click
    if this < 0 then offset = offset + click end
  end
  prev = this
  return this + offset
end
          
            
function read_conf_file (path)
  -- generic Graphite .conf file has parameters a=b separated by name field [name]
  -- returns ordered list of named items with parameters and values { {name=name, parameter=value, ...}, ...}
  -- if name = "pattern" then regular expression escape "\" is converted to Lua pattern escape "%", no other changes
  local ITEM                     -- sticky item
  local result, index = {}, {}
  local function comment (l) return l: match "^%s*%#" end
  local function section  (l)
    local n = l: match "^%s*%[([^%]]+)%]" 
    if n then ITEM = {name = n}; index[n] = ITEM; result[#result+1] = ITEM end
    return n
  end
  local function parameter (l)
    -- syntax:   param (number) = value, number is optional
    local p,n,v = l: match "^%s*([^=%(%s]+)%s*%(?(%d*)%)?%s*=%s*(.-)%s*$"
    if v then 
      v = v: gsub ("[\001-\031]",'')                      -- remove any control characters
      n = tonumber (n)                                    -- there may well not be a numeric parameter  
      if p: match "^%d+$" then p = tonumber (p) end
      if p == "pattern" then v = v: gsub ("\\","%%")      -- both their own escapes!
      elseif v:upper() == "TRUE"  then v = true           -- make true, if that's what it is
      elseif v:upper() == "FALSE" then v = false          -- or false
      else v = tonumber(v) or v end                       -- or number  
      if not ITEM then section "[_anon_]" end             -- create section if none exists
      local item = ITEM[p]
      if item then                                        -- repeated item, make multi-valued table 
        if type(item) ~= "table" then item = {item} end
        item [#item+1] = v
        v = item
      end
      ITEM[p] = v 
    end
  end
  local fh = io.open (path) 
  if fh then
    for line in fh:lines() do 
      local _ = comment(line) or section(line) or parameter(line)
    end
    fh:close ()
  else 
    luup.log ("DataDaemon: unable to open " .. (tostring(path) or '?'))
  end
  return result, index
end

-- pretty (), 
-- pretty-print for Lua
-- 2014.06.26   @akbooer
-- 2015.11.29   use names for global variables (ie. don't expand _G or system libraries)
--              use fully qualified path names for circular references
--              improve formatting of isolated nils in otherwise contiguous numeric arrays
--              improve formatting of nested tables
-- 2016.01.09   fix for {a = false}

function pretty (Lua, name)    -- 2015.11.29   @akbooer
  local con, tab, enc = table.concat, '  ', {}   -- encoded tables (to avoid infinite self-reference loop)
  local function ctrl(y) return ("\\%03d"): format (y:byte ()) end       -- deal with escapes, etc.
  local function str_obj(x) return '"' .. x:gsub ("[\001-\031]", ctrl) .. '"' end
  local function brk_idx(x) return '[' .. tostring(x) .. ']' end
  local function str_idx(x) return x:match "^[%a_][%w_]*$" or brk_idx(str_obj (x)) end
  local function nl (d,x) if x then return '\n'..tab:rep (d),'\n'..tab:rep (d-1) else return '','' end end
  local function val (x, depth, name) 
    if enc[x] then return enc[x] end                                    -- previously encoded
    local t = type(x)
    if t ~= "table" then return (({string = str_obj})[t] or tostring) (x) end
    enc[x] = name                                                       -- start encoding this table
    local idx, its, y = {}, {}, {x[1] or x[2] and true}
    for i in pairs(x) do                                                -- fix isolated nil numeric indices
      y[i] = true; if (type(i) == "number") and x[i+2] then y[i+1] = true end
    end
    for i in ipairs(y) do                                               -- contiguous numeric indices
      y[i] = nil; its[i] = val (x[i], depth+1, con {name,'[',i,']'}) 
    end
    if #its > 0 then its = {con (its, ',')} end                         -- collapse to single line
    for i in pairs(y) do idx[#idx+1] = (x[i] ~= nil) and i end          -- sort remaining non-nil indices
    table.sort (idx, function (a,b) return tostring(a) < tostring (b) end)
    for _,j in ipairs (idx) do                                          -- remaining indices
      local fmt_idx = (({string = str_idx})[type(j)] or brk_idx) (j)
      its[#its+1] = fmt_idx .." = ".. val (x[j], depth+1, name..'.'..fmt_idx) 
    end
    enc [x] = nil                                                       -- finish encoding this table
    local nl1, nl2 = nl(depth, #idx > 1)                                -- indent multiline tables 
    return con {'{', nl1, con {con (its, ','..nl1) }, nl2, '}'}         -- put it all together
  end
  -- pretty()
  for a,b in pairs (_G) do enc[b] = a end                                -- don't encode globals 
  return val(Lua, 1, tostring(name or '_')) 
end 

--
--  set_config {config_override_data}, used to override carbon confiuration
--
function set_config(carbon)
  carbon_conf = carbon
end


-- UDP utility methods

local function open_for_send (ip_and_port)   -- returns socket configured for sending to given destination
  local sock, msg, ok
  local ip, port = ip_and_port: match "(%d+%.%d+%.%d+%.%d+):(%d+)"
  if ip and port then 
    sock, msg = socket.udp()
    if sock then ok, msg = sock:setpeername(ip, port) end         -- connect to destination
  else
    msg = "invalid ip:port syntax '" .. tostring (ip_and_port) .. "'"
  end
  if ok then ok = sock end
  return ok, msg
end

local function open_for_listen (port, listener)             -- sets up listener callbacks for incoming datagrams on port
  local function close () end                               -- TODO: proper close
  local pollrate = 1                                        -- seems to work fine
  local callbackName = "UDP_listener_" .. tostring(port)    -- listener name is unique to port
  local sock, msg, ok = socket.udp()

  local function polling ()
    local datagram, ip 
    repeat
--      datagram = sock:receive()                           -- non-blocking since timeout = 0 
      datagram, ip = sock:receivefrom()                     -- non-blocking since timeout = 0 (also get sender IP)
      if datagram and listener then pcall (listener, datagram, ip) end -- protected call
    until not datagram
    luup.call_delay (callbackName, pollrate , "")     -- continue periodic poll for clients 
  end

  if sock then
    sock:settimeout (0)                           -- don't block! 
    ok, msg = sock:setsockname('*', port)         -- listen for any incoming datagram on port
    if ok then
      _G[callbackName] = polling                  -- set global alias for this callback
      polling()                                   -- start periodic poll for incoming datagrams
      ok = {close = close}
    end
  end
  return ok, msg
end
 
local function open_for_syslog (ip_and_port, tag)
  tag = tag or "DataDaemon"
  local socket, msg = open_for_send(ip_and_port)
  local function send (self, message)                   -- user-callable syslog
    message = message or self or "no message"   
    local user_info = 14                                -- facility = user, severity = info
    local msg = ("<%d>%s %s %s: %s\n"):format (user_info, os.date "%b %d %H:%M:%S", 
                                                          HOST, tag, message) 
    socket: send (msg)
  end
  local syslogSocket
  if socket then syslogSocket = {send = send, close = socket.close} end
  return syslogSocket, msg
end

----
--
-- start(info), create a new daemon
-- info = {configName, UDP_callback, HTTP_callback, Version, child}
-- if carbon_conf exists then don't read from carbon.conf
-- 

function start (this, client)                     -- allow colon or dot notation
  client = client or this
  local syslogSocket    -- if the client calls open_for_syslog, this is the returned syslog object with send and close methods
  local sendSockets = {}
  local stats = {
    sent = 0,
    last_sent = {},
    received = 0,
    last_received = {},
  }
  local httpstats = {
    received = 0,
  }
  local errors = {
      count = 0,
  }
  local STATUS = {                                -- sundry operating stats, including errors
      VERSION = _VERSION,
      ip = myIP (),
      http = httpstats,
      udp = stats,
      errors = errors,
      start_time = os.date "%c",
      client = client.Name,
      destinations = {}
    }
  local config = {DAEMON = STATUS}

  -- UDP & syslog 
  
  local function log (a, b)
    luup.log (table.concat {client.Name or "DataDaemon", ': ', b or a})
  end

  local function error (a, b)
    local errmsg = "ERROR: " .. (b or a or '?')
    log (errmsg)
    errors.count = errors.count + 1
    errors.time = os.date "%c"
    errors.message = errmsg
  end
  
  local function warning (a, b)
    local errmsg = "WARNING: " .. (b or a or '?')
    log (errmsg)
  end
  
  local function syslogSend (a, b)
    if syslogSocket 
      then syslogSocket: send (b or a)
--      else log (a, b) 
    end
  end
  
  local function syslogOpen (ip_port)
    syslogSocket = open_for_syslog (ip_port or '', client.Name or "DataDaemon")
    return syslogSocket or {send = syslogSend, close = function () end}
  end
  
  local function send (a, b)
    stats.sent = stats.sent + 1
    for _, socket in ipairs (sendSockets) do 
      stats.last_sent [table.concat ({socket:getpeername ()},':')] = b or a
      socket:send (b or a) 
    end
  end
  
  local function listenOpen (port, listener)
    local function intercept (msg, ip)
      stats.received = stats.received + 1
      stats.last_received [table.concat{ip or '?', ':', port}] = msg
      local ok, errmsg = pcall (listener, msg, ip)
      if not ok then error (errmsg) end
    end
    local ok, msg = open_for_listen (port, intercept)
    if msg then error (msg) end
    return ok, msg
  end
  
  local function sendOpen (ip_and_port_list)   -- can be called with string of multiple addresses or multiple times
    local sock, msg
    local d = sendSockets
    for ip in (ip_and_port_list or ''): gmatch "%d+%.%d+%.%d+%.%d+%:%d+" do   -- create all the destination sockets
      sock, msg = open_for_send (ip) 
      d[#d+1] = sock
      if sock then table.insert(STATUS.destinations, ip) end
      if msg  then error (msg) end                              -- log error message if failure
    end 
    return {send = send, close = function () end}
  end
         
   -- Configuration routines
  
  local function showConfig () 
    local idx, info = {}, {table.concat {client.Name, " CONFIGURATION at ", os.date "%c"} }
    for a in pairs (config) do idx[#idx+1] = a end
    table.sort (idx)
    for _,a in ipairs (idx) do
      info[#info+1] = table.concat {a, " = ",  pretty (config[a])} 
    end
    return table.concat (info, '\n\n')
  end
  
  -- HTTP routines
  -- handler (request, parameters, outputformat)
  local function local_HTTP_handler (_, lul_parameters)       -- 'global' HTTP handler
    local function CLI ()
      local function noop () end
      local user = client.HTTP_callback or noop
      local reporter = {config = showConfig, diagnostics = showConfig}
      local function show (n,v) return (reporter[v] or user) (n,v) end     -- &show=[config/client]
      local dispatch = {show = show, page = show}
      local N = 0
      local html = {}
      for name, value in pairs (lul_parameters) do
        N = N + 1
        if value == "nil" then value = nil end                        -- make a real nil
        value = tonumber (value) or value                             -- make a real number, if that's what it is
        html[#html+1] = (dispatch[name] or user) (name, value) or ''
      end
      if #html == 0 then html = "No changes made" else html = table.concat (html, '\n') end
      return html
    end
    httpstats.received = httpstats.received + 1
    local ok, html = pcall (CLI)              -- catch any errors
    if not ok then error (html) end           -- report them 
    return html
  end
  
  local function init () 
    local name = client.Name
    if name then
      if not next (carbon_conf) then       -- configuration read from carbon.conf file
        local ok, err, carbon = pcall(read_conf_file, config_path .. "carbon.conf")
        if ok then
          carbon_conf = carbon            -- save the global configuration
        else 
          error (err, 2)
        end
      end
      -- copy carbon.conf to local configuration
      for n, x in pairs (carbon_conf or {}) do
        config['['..n..']'] = x
        x.name = nil
      end
      -- set up HTTP handler
      local callbackName = "HTTP_"..name
      _G[callbackName] = local_HTTP_handler             -- set up global callback alias
      luup.register_handler (callbackName, name)        -- HTTP request handler
    end
  end
   
  -- DataDaemon.start()
  local ok,err = pcall (init)
  if ok then 
    ok = {
      config          = config, 
      cpu_clock       = cpu_clock,
      error           = error, 
      ip              = myIP(),
      warning         = warning,
      log             = log,
      read_conf_file  = read_conf_file,
      open_for_send   = sendOpen, 
      open_for_listen = listenOpen, 
      open_for_syslog = syslogOpen, 
      showConfig      = showConfig,
    }
  else 
    log (err)
  end
  return ok, err
end

-----
